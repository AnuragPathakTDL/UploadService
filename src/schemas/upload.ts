import { z } from "zod";

export const contentClassificationSchema = z.enum(["REEL", "EPISODE"]);

const renditionSchema = z.object({
  name: z.string().min(1),
  codec: z.string().min(1),
  bitrateKbps: z.number().int().positive(),
  resolution: z
    .string()
    .regex(/^[0-9]+x[0-9]+$/, "resolution must be WIDTHxHEIGHT"),
  frameRate: z.number().positive().optional(),
});

export const drmSchema = z.object({
  keyId: z.string(),
  licenseServer: z.string().url(),
});

const readyMetadataSchema = z.object({
  bucket: z.string().min(1),
  manifestObject: z.string().min(1),
  storagePrefix: z.string().min(1).optional(),
  renditions: z.array(renditionSchema).min(1),
  checksum: z.string().min(1),
  signedUrlTtlSeconds: z.number().int().positive().default(300),
  encryption: drmSchema.optional(),
  lifecycle: z
    .object({
      storageClass: z.string().min(1),
      retentionDays: z.number().int().positive().optional(),
    })
    .optional(),
  regionHint: z.string().optional(),
});

export const createUploadUrlBodySchema = z
  .object({
    fileName: z.string().min(1).max(255),
    contentType: z.string().min(3).max(128),
    sizeBytes: z
      .number()
      .int()
      .positive()
      .max(512 * 1024 * 1024),
    assetType: z.enum(["video", "thumbnail", "banner"]),
    contentId: z.string().uuid().optional(),
    contentClassification: contentClassificationSchema.optional(),
  })
  .superRefine((data, ctx) => {
    if (data.assetType === "video" && !data.contentClassification) {
      ctx.addIssue({
        path: ["contentClassification"],
        code: z.ZodIssueCode.custom,
        message: "contentClassification is required for video uploads",
      });
    }
  });

export const createUploadUrlResponseSchema = z.object({
  uploadId: z.string().uuid(),
  uploadUrl: z.string().url(),
  expiresAt: z.string().datetime(),
  objectKey: z.string().min(1),
  storageUrl: z.string().regex(/^gs:\/\//, "storageUrl must be a gs:// URI"),
  fields: z.record(z.string(), z.string()),
  cdn: z.string().optional(),
});

export const uploadStatusResponseSchema = z.object({
  uploadId: z.string().uuid(),
  status: z.enum([
    "REQUESTED",
    "UPLOADING",
    "VALIDATING",
    "PROCESSING",
    "READY",
    "FAILED",
    "EXPIRED",
  ]),
  assetType: z.enum(["video", "thumbnail", "banner"]),
  objectKey: z.string(),
  storageUrl: z
    .string()
    .regex(/^gs:\/\//, "storageUrl must be a gs:// URI")
    .optional(),
  cdnUrl: z.string().url().optional(),
  sizeBytes: z.number().int().nonnegative(),
  contentType: z.string(),
  contentClassification: contentClassificationSchema.optional(),
  expiresAt: z.string().datetime(),
  completedAt: z.string().datetime().optional(),
  failureReason: z.string().optional(),
  validationMeta: z
    .object({
      durationSeconds: z.number().optional(),
      width: z.number().optional(),
      height: z.number().optional(),
      checksum: z.string().optional(),
      bitrateKbps: z.number().int().positive().optional(),
    })
    .optional(),
  processingMeta: z
    .object({
      manifestUrl: z.string().url().optional(),
      defaultThumbnailUrl: z.string().url().optional(),
      previewGeneratedAt: z.string().datetime().optional(),
    })
    .optional(),
});

export const validationCallbackBodySchema = z.object({
  status: z.enum(["success", "failed"]),
  checksum: z.string().optional(),
  durationSeconds: z.number().int().positive().optional(),
  width: z.number().int().positive().optional(),
  height: z.number().int().positive().optional(),
  failureReason: z.string().optional(),
});

export const quotaLimitsSchema = z.object({
  concurrentLimit: z.number().int().positive(),
  dailyLimit: z.number().int().positive(),
  activeUploads: z.number().int().nonnegative(),
  dailyUploads: z.number().int().nonnegative(),
});

export const processingCallbackBodySchema = z
  .object({
    status: z.enum(["ready", "failed"]),
    manifestUrl: z.string().url().optional(),
    defaultThumbnailUrl: z.string().url().optional(),
    bitrateKbps: z.number().int().positive().optional(),
    previewGeneratedAt: z.string().datetime().optional(),
    failureReason: z.string().optional(),
    readyMetadata: readyMetadataSchema.optional(),
  })
  .superRefine((data, ctx) => {
    if (data.status === "ready" && !data.manifestUrl) {
      ctx.addIssue({
        path: ["manifestUrl"],
        code: z.ZodIssueCode.custom,
        message: "manifestUrl is required when status is ready",
      });
    }
    if (data.status === "ready" && !data.readyMetadata) {
      ctx.addIssue({
        path: ["readyMetadata"],
        code: z.ZodIssueCode.custom,
        message: "readyMetadata is required when status is ready",
      });
    }
    if (data.status === "failed" && !data.failureReason) {
      ctx.addIssue({
        path: ["failureReason"],
        code: z.ZodIssueCode.custom,
        message: "failureReason is required when status is failed",
      });
    }
  });

export type CreateUploadUrlBody = z.infer<typeof createUploadUrlBodySchema>;
export type CreateUploadUrlResponse = z.infer<
  typeof createUploadUrlResponseSchema
>;
export type UploadStatusResponse = z.infer<typeof uploadStatusResponseSchema>;
export type ValidationCallbackBody = z.infer<
  typeof validationCallbackBodySchema
>;
export type ProcessingCallbackBody = z.infer<
  typeof processingCallbackBodySchema
>;
export type ReadyMetadata = z.infer<typeof readyMetadataSchema>;
