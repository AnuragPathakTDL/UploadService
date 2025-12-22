import { createHash, randomBytes, randomUUID } from "node:crypto";
import {
  ContentClassification,
  UploadAssetType,
  UploadStatus,
  type Prisma,
  type UploadSession,
} from "@prisma/client";
import type { PubSub } from "@google-cloud/pubsub";
import type { Storage } from "@google-cloud/storage";
import type { FastifyBaseLogger } from "fastify";
import { UploadSessionService } from "./upload-sessions";
import { UploadQuotaService, type QuotaState } from "./quota-service";
import type {
  CreateUploadUrlBody,
  ProcessingCallbackBody,
  ReadyMetadata,
  ValidationCallbackBody,
} from "../schemas/upload";
import { loadConfig, type Env } from "../config";
import type { AuditEvent } from "../types/audit";

const assetPolicies: Record<
  CreateUploadUrlBody["assetType"],
  {
    prefix: string;
    maxSizeBytes: number;
    allowedContentTypes: RegExp[];
  }
> = {
  video: {
    prefix: "videos",
    maxSizeBytes: 512 * 1024 * 1024,
    allowedContentTypes: [/^video\//],
  },
  thumbnail: {
    prefix: "thumbnails",
    maxSizeBytes: 25 * 1024 * 1024,
    allowedContentTypes: [/^image\/(jpeg|png|webp)$/],
  },
  banner: {
    prefix: "banners",
    maxSizeBytes: 25 * 1024 * 1024,
    allowedContentTypes: [/^image\/(jpeg|png|webp)$/],
  },
};

function sanitizeFileName(fileName: string) {
  return fileName
    .trim()
    .replace(/\s+/g, "-")
    .replace(/[^a-zA-Z0-9._-]/g, "_")
    .slice(0, 160);
}

function mapAssetTypeToString(
  assetType: UploadAssetType
): "video" | "thumbnail" | "banner" {
  switch (assetType) {
    case UploadAssetType.VIDEO:
      return "video";
    case UploadAssetType.THUMBNAIL:
      return "thumbnail";
    case UploadAssetType.BANNER:
      return "banner";
    default:
      return "video";
  }
}

interface ReadyForStreamEvent {
  eventId: string;
  eventType: "media.ready-for-stream";
  version: string;
  occurredAt: string;
  data: {
    uploadId: string;
    videoId: string;
    tenantId: string;
    contentType: "REEL" | "EPISODE";
    sourceUpload: {
      storageUrl?: string | null;
      objectKey: string;
      sizeBytes: number;
      contentType: string;
    };
    processedAsset: {
      bucket: string;
      manifestObject: string;
      storagePrefix?: string;
      renditions: ReadyMetadata["renditions"];
      checksum: string;
      signedUrlTtlSeconds: number;
      lifecycle?: ReadyMetadata["lifecycle"];
    };
    encryption?: ReadyMetadata["encryption"];
    ingestRegion: string;
    cdn: {
      defaultBaseUrl: string;
    };
    omeHints: {
      application: "reels" | "episodes";
      protocol: "LL-HLS" | "HLS";
    };
    idempotencyKey: string;
    readyAt: string;
  };
  acknowledgement: {
    deadlineSeconds: number;
    required: boolean;
  };
}

export class UploadManager {
  private readonly config: Env;
  private readonly previewTopic: string | null;
  private readonly processedTopic: string | null;
  private readonly readyTopic: string | null;

  constructor(
    private readonly storage: Storage,
    private readonly sessions: UploadSessionService,
    private readonly quota: UploadQuotaService,
    private readonly pubsub: PubSub,
    private readonly logger: FastifyBaseLogger,
    private readonly auditPublisher?: (event: AuditEvent) => Promise<void>
  ) {
    this.config = loadConfig();
    const previewTopic = this.config.PREVIEW_GENERATION_TOPIC?.trim();
    this.previewTopic = previewTopic ? previewTopic : null;
    const processedTopic = this.config.MEDIA_PROCESSED_TOPIC?.trim();
    this.processedTopic = processedTopic ? processedTopic : null;
    const readyTopic = this.config.MEDIA_READY_FOR_STREAM_TOPIC?.trim();
    this.readyTopic = readyTopic ? readyTopic : null;
  }

  private async emitAudit(event: AuditEvent) {
    if (!this.auditPublisher) {
      return;
    }
    try {
      await this.auditPublisher(event);
    } catch (error) {
      this.logger.error(
        { err: error, eventType: event.type },
        "Failed to publish audit event"
      );
    }
  }

  private validateContent(body: CreateUploadUrlBody) {
    const policy = assetPolicies[body.assetType];
    const isAllowed = policy.allowedContentTypes.some((pattern) =>
      pattern.test(body.contentType)
    );
    if (!isAllowed) {
      throw Object.assign(new Error("unsupported_content_type"), {
        statusCode: 415,
        message: `Unsupported content type ${body.contentType} for ${body.assetType}`,
      });
    }
    if (body.sizeBytes > policy.maxSizeBytes) {
      throw Object.assign(new Error("payload_too_large"), {
        statusCode: 413,
        message: `Maximum allowed size for ${body.assetType} is ${policy.maxSizeBytes} bytes`,
      });
    }
    return policy;
  }

  private buildObjectKey(
    assetType: CreateUploadUrlBody["assetType"],
    fileName: string
  ) {
    const policy = assetPolicies[assetType];
    const sanitizedFileName = sanitizeFileName(fileName);
    return `${policy.prefix}/${Date.now()}-${randomBytes(8).toString("hex")}-${sanitizedFileName}`;
  }

  private mapSessionClassification(
    classification: UploadSession["contentClassification"]
  ): "REEL" | "EPISODE" | null {
    if (!classification) {
      return null;
    }
    return classification === ContentClassification.REEL ? "REEL" : "EPISODE";
  }

  private extractReadyMetadata(session: UploadSession): ReadyMetadata | null {
    const meta = session.validationMeta;
    if (!meta || typeof meta !== "object" || Array.isArray(meta)) {
      return null;
    }
    const readyMetadata = (meta as Record<string, unknown>)["readyMetadata"];
    if (!readyMetadata || typeof readyMetadata !== "object") {
      return null;
    }
    return readyMetadata as ReadyMetadata;
  }

  private buildReadyEventKey(contentId: string, checksum: string) {
    return createHash("sha1").update(`${contentId}:${checksum}`).digest("hex");
  }

  private buildDefaultStoragePrefix(
    contentType: "REEL" | "EPISODE",
    contentId: string
  ) {
    const application = contentType === "REEL" ? "reels" : "episodes";
    return `videos/${contentId}/${application}`;
  }

  async issueUpload(
    body: CreateUploadUrlBody,
    adminId: string,
    correlationId?: string
  ) {
    const policy = this.validateContent(body);
    const now = new Date();
    const objectKey = this.buildObjectKey(body.assetType, body.fileName);
    const expiresAt = new Date(
      now.getTime() + this.config.SIGNED_UPLOAD_TTL_SECONDS * 1000
    );

    let quotaState: QuotaState;
    try {
      quotaState = await this.quota.claim(adminId, now);
    } catch (error) {
      if (error instanceof Error) {
        if (error.message === "concurrent_quota_exceeded") {
          throw Object.assign(new Error("Concurrent upload limit reached"), {
            statusCode: 429,
            code: "concurrent_quota_exceeded",
          });
        }
        if (error.message === "daily_quota_exceeded") {
          throw Object.assign(new Error("Daily upload limit reached"), {
            statusCode: 429,
            code: "daily_quota_exceeded",
          });
        }
      }
      throw error;
    }
    const bucket = this.storage.bucket(this.config.UPLOAD_BUCKET);

    try {
      const [signedPolicy] = await bucket
        .file(objectKey)
        .generateSignedPostPolicyV4({
          expires: expiresAt,
          conditions: [
            ["content-length-range", 1, policy.maxSizeBytes],
            ["eq", "$Content-Type", body.contentType],
            ["eq", "$key", objectKey],
          ],
          fields: {
            key: objectKey,
            "Content-Type": body.contentType,
            success_action_status: "201",
            "x-goog-meta-asset-type": body.assetType,
            "x-goog-meta-content-id": body.contentId ?? "",
          },
        });

      const session = await this.sessions.createSession({
        adminId,
        contentId: body.contentId,
        contentClassification: body.contentClassification,
        assetType: body.assetType,
        objectKey,
        storageUrl: `gs://${this.config.UPLOAD_BUCKET}/${objectKey}`,
        cdnUrl: this.config.CDN_UPLOAD_BASE_URL,
        contentType: body.contentType,
        sizeBytes: body.sizeBytes,
        uploadUrl: signedPolicy.url,
        expiresAt,
        formFields: signedPolicy.fields,
      });

      await this.sessions.markUploading(session.id);

      this.logger.info(
        {
          uploadId: session.id,
          adminId,
          assetType: body.assetType,
          sizeBytes: body.sizeBytes,
          correlationId,
          quotaState,
          audit: {
            action: "upload.intent.created",
            uploadId: session.id,
            adminId,
            contentId: body.contentId,
            objectKey,
            assetType: body.assetType,
            sizeBytes: body.sizeBytes,
            contentClassification: body.contentClassification,
          },
        },
        "Issued signed upload policy"
      );

      void this.emitAudit({
        type: "upload.intent.created",
        uploadId: session.id,
        adminId,
        contentId: body.contentId,
        storageKey: objectKey,
        correlationId,
        metadata: {
          assetType: body.assetType,
          sizeBytes: body.sizeBytes,
          quotaState,
          contentClassification: body.contentClassification,
        },
      });

      return {
        uploadId: session.id,
        uploadUrl: signedPolicy.url,
        expiresAt: expiresAt.toISOString(),
        objectKey,
        storageUrl: `gs://${this.config.UPLOAD_BUCKET}/${objectKey}`,
        fields: signedPolicy.fields,
        cdn: this.config.CDN_UPLOAD_BASE_URL,
      };
    } catch (error) {
      await this.quota.release(adminId);
      throw error;
    }
  }

  async getStatus(uploadId: string, forAdminId?: string) {
    const session = await this.sessions.getSession(uploadId);
    if (!session) {
      return null;
    }

    if (forAdminId && session.adminId !== forAdminId) {
      return null;
    }

    const rawMeta = session.validationMeta as Record<string, unknown> | null;

    const validationMeta = rawMeta
      ? {
          durationSeconds: rawMeta["durationSeconds"] as number | undefined,
          width: rawMeta["width"] as number | undefined,
          height: rawMeta["height"] as number | undefined,
          checksum: rawMeta["checksum"] as string | undefined,
          bitrateKbps: rawMeta["bitrateKbps"] as number | undefined,
        }
      : undefined;

    const processingMeta = rawMeta
      ? {
          manifestUrl: rawMeta["manifestUrl"] as string | undefined,
          defaultThumbnailUrl: rawMeta["defaultThumbnailUrl"] as
            | string
            | undefined,
          previewGeneratedAt: rawMeta["previewGeneratedAt"] as
            | string
            | undefined,
        }
      : undefined;

    const sanitizedValidationMeta =
      validationMeta &&
      Object.values(validationMeta).some(
        (value) => typeof value !== "undefined"
      )
        ? validationMeta
        : undefined;

    const sanitizedProcessingMeta =
      processingMeta &&
      Object.values(processingMeta).some(
        (value) => typeof value !== "undefined"
      )
        ? processingMeta
        : undefined;

    return {
      uploadId: session.id,
      status: session.status,
      assetType: mapAssetTypeToString(session.assetType),
      objectKey: session.objectKey,
      storageUrl: session.storageUrl ?? undefined,
      cdnUrl: session.cdnUrl ?? undefined,
      sizeBytes: session.sizeBytes,
      contentType: session.contentType,
      contentClassification:
        this.mapSessionClassification(session.contentClassification) ??
        undefined,
      expiresAt: session.expiresAt.toISOString(),
      completedAt: session.completedAt?.toISOString(),
      failureReason: session.failureReason ?? undefined,
      validationMeta: sanitizedValidationMeta,
      processingMeta: sanitizedProcessingMeta,
    };
  }

  private async publishMediaUploaded(session: UploadSession) {
    const topicName = this.config.MEDIA_UPLOADED_TOPIC;
    const message = {
      uploadId: session.id,
      objectKey: session.objectKey,
      storageUrl: session.storageUrl,
      cdnUrl: session.cdnUrl,
      assetType: mapAssetTypeToString(session.assetType),
      adminId: session.adminId,
      contentId: session.contentId,
      contentClassification: this.mapSessionClassification(
        session.contentClassification
      ),
      sizeBytes: session.sizeBytes,
      contentType: session.contentType,
      validation: session.validationMeta,
      emittedAt: new Date().toISOString(),
    };

    await this.pubsub.topic(topicName).publishMessage({
      json: message,
    });

    this.logger.info(
      {
        uploadId: session.id,
        assetType: mapAssetTypeToString(session.assetType),
        adminId: session.adminId,
        audit: {
          action: "upload.event.media_uploaded",
          uploadId: session.id,
          adminId: session.adminId,
          contentId: session.contentId,
          objectKey: session.objectKey,
        },
      },
      "Emitted media.uploaded event"
    );

    void this.emitAudit({
      type: "upload.event.media_uploaded",
      uploadId: session.id,
      adminId: session.adminId,
      contentId: session.contentId ?? undefined,
      storageKey: session.objectKey,
      metadata: {
        assetType: mapAssetTypeToString(session.assetType),
        storageUrl: session.storageUrl,
        cdnUrl: session.cdnUrl,
        contentClassification: this.mapSessionClassification(
          session.contentClassification
        ),
      },
    });
  }

  private async publishPreviewRequest(session: UploadSession) {
    if (!this.previewTopic) {
      return;
    }
    if (session.assetType !== UploadAssetType.VIDEO) {
      return;
    }

    const message = {
      uploadId: session.id,
      objectKey: session.objectKey,
      storageUrl: session.storageUrl,
      adminId: session.adminId,
      contentId: session.contentId,
      requestedAt: new Date().toISOString(),
    };

    await this.pubsub.topic(this.previewTopic).publishMessage({
      json: message,
    });

    this.logger.info(
      {
        uploadId: session.id,
        audit: {
          action: "upload.preview.requested",
          uploadId: session.id,
          adminId: session.adminId,
          objectKey: session.objectKey,
        },
      },
      "Published preview generation request"
    );

    void this.emitAudit({
      type: "upload.preview.requested",
      uploadId: session.id,
      adminId: session.adminId,
      contentId: session.contentId ?? undefined,
      storageKey: session.objectKey,
      metadata: {
        storageUrl: session.storageUrl,
      },
    });
  }

  private async publishMediaProcessed(session: UploadSession) {
    if (!this.processedTopic) {
      return;
    }
    if (session.status !== UploadStatus.READY) {
      return;
    }

    const meta = ((session.validationMeta as Record<string, unknown> | null) ??
      {}) as Record<string, unknown>;
    const readyMetadata = this.extractReadyMetadata(session);

    const message = {
      uploadId: session.id,
      objectKey: session.objectKey,
      storageUrl: session.storageUrl,
      cdnUrl: session.cdnUrl,
      adminId: session.adminId,
      contentId: session.contentId,
      assetType: mapAssetTypeToString(session.assetType),
      manifestUrl: meta["manifestUrl"] as string | undefined,
      defaultThumbnailUrl: meta["defaultThumbnailUrl"] as string | undefined,
      durationSeconds: meta["durationSeconds"] as number | undefined,
      bitrateKbps: meta["bitrateKbps"] as number | undefined,
      previewGeneratedAt: meta["previewGeneratedAt"] as string | undefined,
      readyMetadata: readyMetadata ?? undefined,
      emittedAt: new Date().toISOString(),
    };

    await this.pubsub.topic(this.processedTopic).publishMessage({
      json: message,
    });

    this.logger.info(
      {
        uploadId: session.id,
        audit: {
          action: "upload.event.media_processed",
          uploadId: session.id,
          adminId: session.adminId,
          contentId: session.contentId,
          objectKey: session.objectKey,
        },
      },
      "Emitted media.processed event"
    );

    void this.emitAudit({
      type: "upload.event.media_processed",
      uploadId: session.id,
      adminId: session.adminId,
      contentId: session.contentId ?? undefined,
      storageKey: session.objectKey,
      manifestUrl: meta["manifestUrl"] as string | undefined,
      defaultThumbnailUrl: meta["defaultThumbnailUrl"] as string | undefined,
      metadata: {
        durationSeconds: meta["durationSeconds"],
        bitrateKbps: meta["bitrateKbps"],
        previewGeneratedAt: meta["previewGeneratedAt"],
        readyMetadata,
      },
    });
  }

  private async publishMediaReadyForStream(session: UploadSession) {
    if (!this.readyTopic) {
      this.logger.debug(
        { uploadId: session.id },
        "Ready-for-stream topic disabled"
      );
      return;
    }
    if (!session.contentId) {
      this.logger.error(
        { uploadId: session.id },
        "Cannot emit ready-for-stream event without contentId"
      );
      return;
    }

    const readyMetadata = this.extractReadyMetadata(session);
    if (!readyMetadata) {
      this.logger.warn(
        { uploadId: session.id },
        "Ready metadata missing; skipping ready-for-stream event"
      );
      return;
    }

    const contentType = this.mapSessionClassification(
      session.contentClassification
    );
    if (!contentType) {
      this.logger.warn(
        { uploadId: session.id },
        "Content classification missing; cannot publish ready-for-stream"
      );
      return;
    }

    const checksum = readyMetadata.checksum ?? session.validationChecksum;
    if (!checksum) {
      this.logger.warn(
        { uploadId: session.id },
        "Checksum missing; cannot guarantee idempotency"
      );
      return;
    }

    const event: ReadyForStreamEvent = {
      eventId: randomUUID(),
      eventType: "media.ready-for-stream",
      version: "2025-01-01",
      occurredAt: new Date().toISOString(),
      data: {
        uploadId: session.id,
        videoId: session.contentId,
        tenantId: this.config.DEFAULT_TENANT_ID,
        contentType,
        sourceUpload: {
          storageUrl: session.storageUrl,
          objectKey: session.objectKey,
          sizeBytes: session.sizeBytes,
          contentType: session.contentType,
        },
        processedAsset: {
          bucket: readyMetadata.bucket,
          manifestObject: readyMetadata.manifestObject,
          storagePrefix:
            readyMetadata.storagePrefix ??
            this.buildDefaultStoragePrefix(contentType, session.contentId),
          renditions: readyMetadata.renditions,
          checksum,
          signedUrlTtlSeconds: readyMetadata.signedUrlTtlSeconds,
          lifecycle: readyMetadata.lifecycle,
        },
        encryption: readyMetadata.encryption,
        ingestRegion:
          readyMetadata.regionHint ?? this.config.DEFAULT_INGEST_REGION,
        cdn: {
          defaultBaseUrl: session.cdnUrl ?? this.config.CDN_UPLOAD_BASE_URL,
        },
        omeHints: {
          application: contentType === "REEL" ? "reels" : "episodes",
          protocol: contentType === "REEL" ? "LL-HLS" : "HLS",
        },
        idempotencyKey: this.buildReadyEventKey(session.contentId, checksum),
        readyAt: session.completedAt
          ? session.completedAt.toISOString()
          : new Date().toISOString(),
      },
      acknowledgement: {
        deadlineSeconds: 60,
        required: true,
      },
    };

    await this.pubsub.topic(this.readyTopic).publishMessage({ json: event });

    this.logger.info(
      {
        uploadId: session.id,
        contentId: session.contentId,
        bucket: readyMetadata.bucket,
        manifestObject: readyMetadata.manifestObject,
      },
      "Emitted media.ready-for-stream event"
    );

    void this.emitAudit({
      type: "upload.event.media_ready_for_stream",
      uploadId: session.id,
      adminId: session.adminId,
      contentId: session.contentId ?? undefined,
      storageKey: session.objectKey,
      metadata: {
        bucket: readyMetadata.bucket,
        manifestObject: readyMetadata.manifestObject,
        renditions: readyMetadata.renditions,
        ingestRegion:
          readyMetadata.regionHint ?? this.config.DEFAULT_INGEST_REGION,
        contentClassification: contentType,
      },
    });
  }

  async handleValidation(
    uploadId: string,
    payload: ValidationCallbackBody,
    source: string
  ) {
    const session = await this.sessions.getSession(uploadId);
    if (!session) {
      throw Object.assign(new Error("upload_not_found"), {
        statusCode: 404,
      });
    }

    await this.sessions.markValidating(uploadId);

    const success = payload.status === "success";
    const updated = await this.sessions.completeValidation(uploadId, success, {
      checksum: payload.checksum,
      durationSeconds: payload.durationSeconds,
      width: payload.width,
      height: payload.height,
      failureReason: payload.failureReason,
    });

    if (!success) {
      await this.quota.release(session.adminId);
      this.logger.warn(
        {
          uploadId,
          adminId: session.adminId,
          audit: {
            action: "upload.validation.failed",
            uploadId,
            adminId: session.adminId,
            reason: payload.failureReason,
            source,
          },
        },
        "Upload validation failed"
      );
      void this.emitAudit({
        type: "upload.validation.failed",
        uploadId,
        adminId: session.adminId,
        contentId: session.contentId ?? undefined,
        storageKey: session.objectKey,
        metadata: {
          reason: payload.failureReason,
          source,
        },
      });
      return updated;
    }

    await this.publishMediaUploaded(updated);
    await this.publishPreviewRequest(updated);

    this.logger.info(
      {
        uploadId,
        adminId: session.adminId,
        audit: {
          action: "upload.validation.passed",
          uploadId,
          adminId: session.adminId,
          checksum: payload.checksum,
          source,
        },
      },
      "Upload validation completed"
    );
    void this.emitAudit({
      type: "upload.validation.passed",
      uploadId,
      adminId: session.adminId,
      contentId: session.contentId ?? undefined,
      storageKey: session.objectKey,
      metadata: {
        checksum: payload.checksum,
        source,
      },
    });
    return updated;
  }

  async markProcessingComplete(
    uploadId: string,
    payload: ProcessingCallbackBody
  ) {
    const session = await this.sessions.getSession(uploadId);
    if (!session) {
      throw Object.assign(new Error("upload_not_found"), {
        statusCode: 404,
      });
    }

    const ready = payload.status === "ready";
    const readyMetadata = ready ? payload.readyMetadata : undefined;
    const previewGeneratedAt = ready
      ? (payload.previewGeneratedAt ?? new Date().toISOString())
      : undefined;
    const existingMeta =
      session.validationMeta &&
      typeof session.validationMeta === "object" &&
      !Array.isArray(session.validationMeta)
        ? (session.validationMeta as Prisma.JsonObject)
        : undefined;

    const updated = await this.sessions.updateProcessingOutcome(uploadId, {
      ready,
      manifestUrl: payload.status === "ready" ? payload.manifestUrl : undefined,
      defaultThumbnailUrl:
        payload.status === "ready" ? payload.defaultThumbnailUrl : undefined,
      bitrateKbps: payload.status === "ready" ? payload.bitrateKbps : undefined,
      previewGeneratedAt,
      failureReason:
        payload.status === "failed" ? payload.failureReason : undefined,
      existingMeta,
      readyMetadata,
    });

    await this.quota.release(session.adminId);

    if (ready) {
      this.logger.info(
        {
          uploadId,
          adminId: session.adminId,
          objectKey: session.objectKey,
          audit: {
            action: "upload.processing.ready",
            uploadId,
            adminId: session.adminId,
            manifestUrl: payload.manifestUrl,
            objectKey: session.objectKey,
            defaultThumbnailUrl: payload.defaultThumbnailUrl,
          },
        },
        "Upload marked READY"
      );
      void this.emitAudit({
        type: "upload.processing.ready",
        uploadId,
        adminId: session.adminId,
        contentId: session.contentId ?? undefined,
        storageKey: session.objectKey,
        manifestUrl: payload.manifestUrl,
        defaultThumbnailUrl: payload.defaultThumbnailUrl,
        metadata: {
          status: "ready",
          bitrateKbps: payload.bitrateKbps,
          previewGeneratedAt,
          readyMetadata,
        },
      });
      await this.publishMediaProcessed(updated);
      await this.publishMediaReadyForStream(updated);
    } else {
      this.logger.warn(
        {
          uploadId,
          failureReason: payload.failureReason,
          adminId: session.adminId,
          objectKey: session.objectKey,
          audit: {
            action: "upload.processing.failed",
            uploadId,
            adminId: session.adminId,
            reason: payload.failureReason,
            objectKey: session.objectKey,
            manifestUrl: payload.manifestUrl,
            defaultThumbnailUrl: payload.defaultThumbnailUrl,
          },
        },
        "Upload failed during processing"
      );
      void this.emitAudit({
        type: "upload.processing.failed",
        uploadId,
        adminId: session.adminId,
        contentId: session.contentId ?? undefined,
        storageKey: session.objectKey,
        manifestUrl: payload.manifestUrl,
        defaultThumbnailUrl: payload.defaultThumbnailUrl,
        metadata: {
          status: "failed",
          reason: payload.failureReason,
          bitrateKbps: payload.bitrateKbps,
          previewGeneratedAt,
        },
      });
    }

    return updated;
  }

  async expireStale(now: Date) {
    const expired = await this.sessions.expireSessionsOlderThan(now);
    for (const session of expired) {
      await this.quota.release(session.adminId);
    }
    if (expired.length > 0) {
      this.logger.warn(
        { expired: expired.length },
        "Expired stale upload sessions"
      );
    }
    return expired.length;
  }
}
