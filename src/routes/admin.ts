import fp from "fastify-plugin";
import type { FastifyInstance, FastifyRequest } from "fastify";
import { z } from "zod";
import {
  createUploadUrlBodySchema,
  createUploadUrlResponseSchema,
  uploadStatusResponseSchema,
  quotaLimitsSchema,
} from "../schemas/upload";

const uploadIdParamSchema = z.object({
  uploadId: z.string().uuid(),
});

function ensureAdmin(request: FastifyRequest) {
  const adminIdHeader = request.headers["x-pocketlol-admin-id"];
  const rolesHeader = request.headers["x-pocketlol-admin-roles"];

  const adminId = Array.isArray(adminIdHeader)
    ? adminIdHeader[0]
    : adminIdHeader;
  const rolesString = Array.isArray(rolesHeader) ? rolesHeader[0] : rolesHeader;
  const roles =
    rolesString
      ?.split(",")
      .map((role) => role.trim().toLowerCase())
      .filter(Boolean) ?? [];

  if (!adminId) {
    throw request.server.httpErrors.unauthorized("Missing admin identity");
  }

  if (!roles.includes("admin")) {
    throw request.server.httpErrors.forbidden("Admin role required");
  }

  return { adminId, roles };
}

export default fp(async function adminRoutes(fastify: FastifyInstance) {
  fastify.addHook("preHandler", async (request, reply) => {
    await fastify.verifyServiceRequest(request, reply);
  });

  fastify.post(
    "/uploads/sign",
    {
      schema: {
        body: createUploadUrlBodySchema,
        response: {
          200: createUploadUrlResponseSchema,
        },
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
        response: {
          200: uploadStatusResponseSchema,
        },
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
      schema: {
        response: {
          200: quotaLimitsSchema,
        },
      },
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
});
