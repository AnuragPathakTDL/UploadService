import type { FastifyInstance, FastifyRequest } from "fastify";
import { createUploadUrlBodySchema } from "../schemas/upload";

function resolveAdminId(request: FastifyRequest) {
  const adminHeader = request.headers["x-pocketlol-admin-id"];
  const adminId = Array.isArray(adminHeader) ? adminHeader[0] : adminHeader;
  if (!adminId) {
    throw request.server.httpErrors.badRequest(
      "x-pocketlol-admin-id header required"
    );
  }
  return adminId;
}

export default async function internalRoutes(fastify: FastifyInstance) {
  fastify.post(
    "/uploads/sign",
    {
      schema: {
        body: createUploadUrlBodySchema,
      },
    },
    async (request) => {
      const adminId = resolveAdminId(request);
      const body = createUploadUrlBodySchema.parse(request.body);
      try {
        return await fastify.uploadManager.issueUpload(
          body,
          adminId,
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
          { err: error },
          "Failed to issue internal upload URL"
        );
        throw fastify.httpErrors.internalServerError(
          "Failed to issue upload URL"
        );
      }
    }
  );
}
