import { randomBytes } from "node:crypto";
import fp from "fastify-plugin";
import type { FastifyInstance } from "fastify";
import {
  createUploadUrlBodySchema,
  createUploadUrlResponseSchema,
  type CreateUploadUrlResponse,
} from "../schemas/upload";
import { loadConfig } from "../config";

function buildResponse(
  bucket: string,
  baseUrl: string,
  ttlSeconds: number,
  fileName: string
): CreateUploadUrlResponse {
  const keySuffix = randomBytes(6).toString("hex");
  const sanitizedFileName = fileName.replace(/[^a-zA-Z0-9._-]/g, "_");
  const objectKey = `${Date.now()}-${keySuffix}-${sanitizedFileName}`;
  const uploadUrl = `${baseUrl.replace(/\/$/, "")}/${bucket}/${objectKey}`;
  const expiresAt = new Date(Date.now() + ttlSeconds * 1000).toISOString();
  return {
    uploadUrl,
    expiresAt,
    fields: {
      key: objectKey,
      bucket,
      policy: randomBytes(12).toString("base64url"),
      signature: randomBytes(32).toString("hex"),
    },
    cdn: new URL(baseUrl).host,
  };
}

export default fp(async function internalRoutes(fastify: FastifyInstance) {
  const config = loadConfig();

  fastify.post("/uploads/videos", {
    schema: {
      body: createUploadUrlBodySchema,
      response: {
        200: createUploadUrlResponseSchema,
      },
    },
    handler: async (request) => {
      const body = createUploadUrlBodySchema.parse(request.body);
      const response = buildResponse(
        config.UPLOAD_BUCKET,
        config.CDN_UPLOAD_BASE_URL,
        config.SIGNED_UPLOAD_TTL_SECONDS,
        body.fileName
      );
      request.log.info(
        { fileName: body.fileName, sizeBytes: body.sizeBytes },
        "Generated upload URL"
      );
      return response;
    },
  });
});
