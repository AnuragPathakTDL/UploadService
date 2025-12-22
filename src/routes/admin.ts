import type { FastifyInstance, FastifyRequest } from "fastify";
import { z } from "zod";
import { createUploadUrlBodySchema } from "../schemas/upload";

const uploadIdParamSchema = z.object({
  uploadId: z.string().uuid(),
});

function ensureAdmin(request: FastifyRequest) {
  const adminIdHeader = request.headers["x-pocketlol-admin-id"];
  const userTypeHeader =
    request.headers["x-pocketlol-user-type"] ?? request.headers["x-user-type"];

  const adminId = Array.isArray(adminIdHeader)
    ? adminIdHeader[0]
    : adminIdHeader;
  const userType = Array.isArray(userTypeHeader)
    ? userTypeHeader[0]
    : userTypeHeader;

  if (!adminId) {
    throw request.server.httpErrors.unauthorized("Missing admin identity");
  }

  if (!userType || userType.toUpperCase() !== "ADMIN") {
    throw request.server.httpErrors.forbidden("Admin user required");
  }

  return { adminId };
}

export default async function adminRoutes(fastify: FastifyInstance) {
  fastify.addHook("preHandler", async (request, reply) => {
    await fastify.verifyServiceRequest(request, reply);
  });

  fastify.post(
    "/uploads/sign",
    {
      schema: {
        body: createUploadUrlBodySchema,
      },
    },
    async (request) => {
      const adminContext = ensureAdmin(request);
      const body = createUploadUrlBodySchema.parse(request.body);
      try {
        return await fastify.uploadManager.issueUpload(
          body,
          adminContext.adminId,
          request.id
        );
      } catch (error) {
        if (
          error instanceof Error &&
          (error as { statusCode?: number }).statusCode
        ) {
          throw error;
        }
        request.log.error(
          { err: error, adminId: adminContext.adminId },
          "Failed to issue signed upload URL"
        );
        throw fastify.httpErrors.internalServerError(
          "Failed to issue upload URL"
        );
      }
    }
  );

  fastify.get(
    "/uploads/:uploadId/status",
    {
      schema: {
        params: uploadIdParamSchema,
      },
    },
    async (request) => {
      const adminContext = ensureAdmin(request);
      const params = uploadIdParamSchema.parse(request.params);
      const status = await fastify.uploadManager.getStatus(
        params.uploadId,
        adminContext.adminId
      );
      if (!status) {
        throw fastify.httpErrors.notFound("Upload not found");
      }
      return status;
    }
  );

  fastify.get(
    "/uploads/quota",
    {
      schema: {},
    },
    async (request) => {
      const adminContext = ensureAdmin(request);
      const now = new Date();
      const current = await fastify.uploadQuota.getCurrentQuota(
        adminContext.adminId,
        now
      );
      const limits = fastify.uploadQuota.getLimits();
      return {
        concurrentLimit: limits.concurrentLimit,
        dailyLimit: limits.dailyLimit,
        activeUploads: current.activeUploads,
        dailyUploads: current.dailyUploads,
      };
    }
  );
}
