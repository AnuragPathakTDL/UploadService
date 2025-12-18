import fp from "fastify-plugin";
import { Storage, type StorageOptions } from "@google-cloud/storage";
import type { FastifyInstance } from "fastify";
import { loadConfig, type Env } from "../config";

function createStorageOptions(config: Env): StorageOptions {
  const options: StorageOptions = {};
  if (config.GCP_PROJECT_ID) {
    options.projectId = config.GCP_PROJECT_ID;
  }
  if (config.GCP_SERVICE_ACCOUNT_KEY) {
    try {
      const decoded = Buffer.from(
        config.GCP_SERVICE_ACCOUNT_KEY,
        "base64"
      ).toString("utf8");
      options.credentials = JSON.parse(
        decoded
      ) as StorageOptions["credentials"];
    } catch (error) {
      throw new Error(
        "GCP_SERVICE_ACCOUNT_KEY must be a base64-encoded JSON service account credential"
      );
    }
  }
  return options;
}

declare module "fastify" {
  interface FastifyInstance {
    storage: Storage;
  }
}

async function storagePlugin(fastify: FastifyInstance) {
  const config = loadConfig();
  const storage = new Storage(createStorageOptions(config));
  fastify.decorate("storage", storage);
}

export default fp(storagePlugin, {
  name: "storage",
});
