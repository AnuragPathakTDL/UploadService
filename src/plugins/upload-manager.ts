import fp from "fastify-plugin";
import type { FastifyInstance } from "fastify";
import { UploadManager } from "../services/upload-manager";

declare module "fastify" {
  interface FastifyInstance {
    uploadManager: UploadManager;
  }
}

async function uploadManagerPlugin(fastify: FastifyInstance) {
  const manager = new UploadManager(
    fastify.storage,
    fastify.uploadSessions,
    fastify.uploadQuota,
    fastify.pubsub,
    fastify.log,
    fastify.publishAuditEvent
  );
  fastify.decorate("uploadManager", manager);
}

export default fp(uploadManagerPlugin, {
  name: "upload-manager",
  dependencies: [
    "audit",
    "storage",
    "upload-sessions",
    "upload-quota",
    "pubsub",
  ],
});
