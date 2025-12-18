import fp from "fastify-plugin";
import type { FastifyInstance } from "fastify";
import { UploadSessionService } from "../services/upload-sessions";

declare module "fastify" {
  interface FastifyInstance {
    uploadSessions: UploadSessionService;
  }
}

async function uploadSessionsPlugin(fastify: FastifyInstance) {
  const service = new UploadSessionService(fastify.prisma);
  fastify.decorate("uploadSessions", service);
}

export default fp(uploadSessionsPlugin, {
  name: "upload-sessions",
  dependencies: ["prisma"],
});
