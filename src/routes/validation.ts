import type { FastifyInstance } from "fastify";
import { z } from "zod";
import {
  validationCallbackBodySchema,
  processingCallbackBodySchema,
} from "../schemas/upload";

const uploadIdParamSchema = z.object({
  uploadId: z.string().uuid(),
});

export default async function validationRoutes(fastify: FastifyInstance) {
  fastify.post(
    "/uploads/:uploadId/validation",
    {
      schema: {
        params: uploadIdParamSchema,
        body: validationCallbackBodySchema,
      },
    },
    async (request) => {
      const params = uploadIdParamSchema.parse(request.params);
      const body = validationCallbackBodySchema.parse(request.body);
      await fastify.uploadManager.handleValidation(
        params.uploadId,
        body,
        request.id
      );
      return { status: "accepted" as const };
    }
  );

  fastify.post(
    "/uploads/:uploadId/processing",
    {
      schema: {
        params: uploadIdParamSchema,
        body: processingCallbackBodySchema,
      },
    },
    async (request) => {
      const params = uploadIdParamSchema.parse(request.params);
      const body = processingCallbackBodySchema.parse(request.body);
      await fastify.uploadManager.markProcessingComplete(params.uploadId, {
        manifestUrl: body.status === "ready" ? body.manifestUrl : undefined,
        defaultThumbnailUrl:
          body.status === "ready" ? body.defaultThumbnailUrl : undefined,
        bitrateKbps: body.status === "ready" ? body.bitrateKbps : undefined,
        previewGeneratedAt:
          body.status === "ready" ? body.previewGeneratedAt : undefined,
        failureReason:
          body.status === "failed" ? body.failureReason : undefined,
      });
      return { status: "accepted" as const };
    }
  );
}
