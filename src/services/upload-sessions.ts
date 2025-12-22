import {
  ContentClassification,
  Prisma,
  UploadAssetType,
  UploadStatus,
  type PrismaClient,
  type UploadSession,
} from "@prisma/client";
import type { ReadyMetadata } from "../schemas/upload";

export type CreateUploadSessionInput = {
  adminId: string;
  contentId?: string;
  assetType: "video" | "thumbnail" | "banner";
  contentClassification?: "REEL" | "EPISODE";
  objectKey: string;
  storageUrl: string;
  cdnUrl?: string;
  contentType: string;
  sizeBytes: number;
  uploadUrl: string;
  expiresAt: Date;
  formFields: Record<string, string>;
};

export type UploadValidationResult = {
  checksum?: string;
  durationSeconds?: number;
  width?: number;
  height?: number;
  failureReason?: string;
};

export class UploadSessionService {
  constructor(private readonly prisma: PrismaClient) {}

  private mapAssetType(assetType: CreateUploadSessionInput["assetType"]) {
    switch (assetType) {
      case "video":
        return UploadAssetType.VIDEO;
      case "thumbnail":
        return UploadAssetType.THUMBNAIL;
      case "banner":
        return UploadAssetType.BANNER;
      default:
        throw new Error(`Unsupported asset type: ${assetType as string}`);
    }
  }

  private mapClassification(
    classification: CreateUploadSessionInput["contentClassification"]
  ) {
    if (!classification) {
      return undefined;
    }
    return classification === "REEL"
      ? ContentClassification.REEL
      : ContentClassification.EPISODE;
  }

  async createSession(input: CreateUploadSessionInput) {
    return this.prisma.uploadSession.create({
      data: {
        adminId: input.adminId,
        contentId: input.contentId,
        contentClassification: this.mapClassification(
          input.contentClassification
        ),
        assetType: this.mapAssetType(input.assetType),
        objectKey: input.objectKey,
        storageUrl: input.storageUrl,
        cdnUrl: input.cdnUrl,
        contentType: input.contentType,
        sizeBytes: input.sizeBytes,
        uploadUrl: input.uploadUrl,
        expiresAt: input.expiresAt,
        formFields: input.formFields,
        status: UploadStatus.REQUESTED,
      },
    });
  }

  async markUploading(id: string) {
    return this.prisma.uploadSession.update({
      where: { id },
      data: { status: UploadStatus.UPLOADING },
    });
  }

  async markValidating(id: string) {
    return this.prisma.uploadSession.update({
      where: { id },
      data: { status: UploadStatus.VALIDATING },
    });
  }

  async completeValidation(
    id: string,
    success: boolean,
    details: UploadValidationResult
  ) {
    return this.prisma.uploadSession.update({
      where: { id },
      data: {
        status: success ? UploadStatus.PROCESSING : UploadStatus.FAILED,
        validationChecksum: details.checksum,
        validationMeta: {
          durationSeconds: details.durationSeconds,
          width: details.width,
          height: details.height,
          checksum: details.checksum,
        },
        failureReason: success ? undefined : details.failureReason,
      },
    });
  }

  async markReady(id: string) {
    return this.prisma.uploadSession.update({
      where: { id },
      data: {
        status: UploadStatus.READY,
        completedAt: new Date(),
      },
    });
  }

  async markFailed(id: string, reason: string) {
    return this.prisma.uploadSession.update({
      where: { id },
      data: {
        status: UploadStatus.FAILED,
        failureReason: reason,
      },
    });
  }

  async updateProcessingOutcome(
    id: string,
    outcome: {
      manifestUrl?: string;
      defaultThumbnailUrl?: string;
      bitrateKbps?: number;
      previewGeneratedAt?: string;
      ready: boolean;
      failureReason?: string;
      existingMeta?: Prisma.JsonObject;
      readyMetadata?: ReadyMetadata;
    }
  ) {
    const nextMeta: Prisma.JsonObject = {
      ...(outcome.existingMeta ?? {}),
    };

    if (outcome.ready) {
      if (typeof outcome.manifestUrl !== "undefined") {
        nextMeta.manifestUrl = outcome.manifestUrl;
      }
      if (typeof outcome.defaultThumbnailUrl !== "undefined") {
        nextMeta.defaultThumbnailUrl = outcome.defaultThumbnailUrl;
      }
      if (typeof outcome.previewGeneratedAt !== "undefined") {
        nextMeta.previewGeneratedAt = outcome.previewGeneratedAt;
      }
      if (typeof outcome.bitrateKbps !== "undefined") {
        nextMeta.bitrateKbps = outcome.bitrateKbps;
      }
    } else {
      delete nextMeta.manifestUrl;
      delete nextMeta.defaultThumbnailUrl;
      delete nextMeta.previewGeneratedAt;
      if (typeof outcome.bitrateKbps !== "undefined") {
        nextMeta.bitrateKbps = outcome.bitrateKbps;
      } else {
        delete nextMeta.bitrateKbps;
      }
    }

    if (outcome.readyMetadata) {
      nextMeta.readyMetadata = outcome.readyMetadata;
    } else {
      delete nextMeta.readyMetadata;
    }

    const metadataPayload: Prisma.InputJsonValue | undefined =
      Object.keys(nextMeta).length > 0 ? nextMeta : undefined;

    return this.prisma.uploadSession.update({
      where: { id },
      data: {
        status: outcome.ready ? UploadStatus.READY : UploadStatus.FAILED,
        completedAt: outcome.ready ? new Date() : undefined,
        failureReason: outcome.ready ? undefined : outcome.failureReason,
        validationMeta: metadataPayload,
      },
    });
  }

  async expireSessionsOlderThan(now: Date) {
    const staleSessions = await this.prisma.uploadSession.findMany({
      where: {
        status: {
          in: [
            UploadStatus.REQUESTED,
            UploadStatus.UPLOADING,
            UploadStatus.VALIDATING,
          ],
        },
        expiresAt: {
          lt: now,
        },
      },
    });

    if (staleSessions.length === 0) {
      return [] as UploadSession[];
    }

    await this.prisma.$transaction(
      staleSessions.map((session) =>
        this.prisma.uploadSession.update({
          where: { id: session.id },
          data: {
            status: UploadStatus.EXPIRED,
            failureReason: "Upload session expired",
          },
        })
      )
    );

    return staleSessions;
  }

  async getSession(id: string) {
    return this.prisma.uploadSession.findUnique({
      where: { id },
    });
  }

  async findByObjectKey(objectKey: string) {
    return this.prisma.uploadSession.findUnique({
      where: { objectKey },
    });
  }

  async listActiveByAdmin(adminId: string) {
    return this.prisma.uploadSession.findMany({
      where: {
        adminId,
        status: {
          in: [
            UploadStatus.REQUESTED,
            UploadStatus.UPLOADING,
            UploadStatus.VALIDATING,
            UploadStatus.PROCESSING,
          ],
        },
      },
    });
  }
}
