import Fastify from "fastify";
import {
  serializerCompiler,
  validatorCompiler,
  ZodTypeProvider,
} from "fastify-type-provider-zod";
import sensible from "@fastify/sensible";
import helmet from "@fastify/helmet";
import cors from "@fastify/cors";
import { loadConfig } from "./config";
import serviceAuthPlugin from "./plugins/service-auth";
import auditPlugin from "./plugins/audit";
import prismaPlugin from "./plugins/prisma";
import redisPlugin from "./plugins/redis";
import pubsubPlugin from "./plugins/pubsub";
import uploadSessionsPlugin from "./plugins/upload-sessions";
import quotaPlugin from "./plugins/quota";
import storagePlugin from "./plugins/storage";
import uploadManagerPlugin from "./plugins/upload-manager";
import internalRoutes from "./routes/internal";
import adminRoutes from "./routes/admin";
import validationRoutes from "./routes/validation";
import { startObservability, shutdownObservability } from "./observability";

export async function buildApp() {
  const config = loadConfig();
  await startObservability({
    serviceName: "upload-service",
    serviceVersion: process.env.npm_package_version,
    tracesEndpoint: config.OTEL_TRACES_ENDPOINT,
    metricsEndpoint: config.OTEL_METRICS_ENDPOINT,
    metricsExportIntervalMillis: config.OTEL_METRICS_INTERVAL_MS,
  });

  const app = Fastify({
    logger: {
      level: config.LOG_LEVEL,
      transport:
        config.NODE_ENV === "development"
          ? {
              target: "pino-pretty",
              options: {
                colorize: true,
                translateTime: "SYS:standard",
              },
            }
          : undefined,
    },
    trustProxy: true,
    bodyLimit: config.HTTP_BODY_LIMIT,
  }).withTypeProvider<ZodTypeProvider>();

  app.setValidatorCompiler(validatorCompiler);
  app.setSerializerCompiler(serializerCompiler);

  await app.register(sensible);
  await app.register(cors, { origin: false });
  await app.register(helmet, { contentSecurityPolicy: false });
  await app.register(prismaPlugin);
  await app.register(auditPlugin);
  await app.register(uploadSessionsPlugin);
  await app.register(redisPlugin);
  await app.register(quotaPlugin);
  await app.register(storagePlugin);
  await app.register(pubsubPlugin);
  await app.register(uploadManagerPlugin);
  await app.register(serviceAuthPlugin);
  await app.register(internalRoutes, { prefix: `/internal` });

  const externalBase = "/api/v1/upload";
  await app.register(adminRoutes, { prefix: `${externalBase}/admin` });
  await app.register(validationRoutes, { prefix: `${externalBase}/internal` });

  app.get("/health", async () => ({ status: "ok" }));

  let cleanupTimer: NodeJS.Timeout | null = null;

  app.addHook("onReady", async () => {
    cleanupTimer = setInterval(() => {
      void app.uploadManager.expireStale(new Date());
    }, config.CLEANUP_INTERVAL_SECONDS * 1000);
    cleanupTimer.unref();
  });

  app.addHook("onClose", async () => {
    if (cleanupTimer) {
      clearInterval(cleanupTimer);
      cleanupTimer = null;
    }
    await shutdownObservability();
  });

  return app;
}
