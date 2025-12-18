import fp from "fastify-plugin";
import type { FastifyInstance } from "fastify";
import { UploadQuotaService } from "../services/quota-service";
import { loadConfig } from "../config";

declare module "fastify" {
  interface FastifyInstance {
    uploadQuota: UploadQuotaService;
  }
}

async function quotaPlugin(fastify: FastifyInstance) {
  const service = new UploadQuotaService(fastify.redis, loadConfig());
  fastify.decorate("uploadQuota", service);
}

export default fp(quotaPlugin, {
  name: "upload-quota",
  dependencies: ["redis"],
});
