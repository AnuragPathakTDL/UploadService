import fp from "fastify-plugin";
import { request } from "undici";
import type { FastifyInstance } from "fastify";
import { loadConfig } from "../config";
import type { AuditEvent } from "../types/audit";

const DEFAULT_TIMEOUT = 2_000;

async function dispatchAuditEvent(
  fastify: FastifyInstance,
  event: AuditEvent,
  options: {
    enabled: boolean;
    sinkUrl?: string;
    sinkToken?: string;
    timeoutMs: number;
    serviceName: string;
  }
) {
  const occurredAt = event.occurredAt ?? new Date().toISOString();

  if (!options.enabled) {
    fastify.log.debug(
      { eventType: event.type },
      "Audit disabled; event suppressed"
    );
    return;
  }

  if (!options.sinkUrl) {
    fastify.log.info(
      { eventType: event.type, event },
      "Audit sink not configured; event recorded locally"
    );
    return;
  }

  try {
    await request(options.sinkUrl, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...(options.sinkToken
          ? {
              authorization: `Bearer ${options.sinkToken}`,
            }
          : {}),
      },
      body: JSON.stringify({
        ...event,
        occurredAt,
        source: event.source ?? options.serviceName,
      }),
      headersTimeout: options.timeoutMs,
      bodyTimeout: options.timeoutMs,
    });
  } catch (error) {
    fastify.log.error(
      { err: error, eventType: event.type },
      "Failed to publish audit event"
    );
  }
}

async function auditPlugin(fastify: FastifyInstance) {
  const config = loadConfig();
  const enabled = Boolean(config.ENABLE_AUDIT_EVENTS);
  const sinkUrl = config.AUDIT_EVENT_SINK_URL;
  const sinkToken = config.AUDIT_EVENT_SINK_TOKEN;
  const timeout = config.AUDIT_EVENT_SINK_TIMEOUT_MS ?? DEFAULT_TIMEOUT;
  const serviceName = "upload-service";

  fastify.decorate(
    "publishAuditEvent",
    async (event: AuditEvent): Promise<void> => {
      await dispatchAuditEvent(fastify, event, {
        enabled,
        sinkUrl,
        sinkToken,
        timeoutMs: timeout,
        serviceName,
      });
    }
  );
}

declare module "fastify" {
  interface FastifyInstance {
    publishAuditEvent(event: AuditEvent): Promise<void>;
  }
}

export default fp(auditPlugin, { name: "audit" });
