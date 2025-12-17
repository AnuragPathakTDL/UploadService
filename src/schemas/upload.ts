import { z } from "zod";

export const createUploadUrlBodySchema = z.object({
  fileName: z.string().min(1).max(255),
  contentType: z.string().min(3).max(128),
  sizeBytes: z.number().int().positive().max(512 * 1024 * 1024),
});

export const createUploadUrlResponseSchema = z.object({
  uploadUrl: z.string().url(),
  expiresAt: z.string().datetime(),
  fields: z.record(z.string(), z.string()).optional(),
  cdn: z.string().optional(),
});

export type CreateUploadUrlBody = z.infer<typeof createUploadUrlBodySchema>;
export type CreateUploadUrlResponse = z.infer<typeof createUploadUrlResponseSchema>;
