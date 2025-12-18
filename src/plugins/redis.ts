import fp from "fastify-plugin";
import Redis from "ioredis";
import type { FastifyInstance } from "fastify";
import { loadConfig } from "../config";

declare module "fastify" {
  interface FastifyInstance {
    redis: Redis;
  }
}

async function redisPlugin(fastify: FastifyInstance) {
  const config = loadConfig();
  const redis = new Redis(config.REDIS_URL, {
    lazyConnect: true,
  });

  fastify.decorate("redis", redis);

  fastify.addHook("onReady", async () => {
    await redis.connect();
  });

  fastify.addHook("onClose", async () => {
    await redis.quit();
  });
}

export default fp(redisPlugin, {
  name: "redis",
});
