import fp from "fastify-plugin";
import type { FastifyInstance, FastifyReply, FastifyRequest } from "fastify";
import { loadConfig } from "../config";

function extractToken(request: FastifyRequest) {
  const bearer = request.headers.authorization;
  if (typeof bearer === "string" && bearer.startsWith("Bearer ")) {
    return bearer.slice("Bearer ".length).trim();
  }
  const headerToken = request.headers["x-service-token"];
  if (Array.isArray(headerToken)) {
    return headerToken[0];
  }
  if (typeof headerToken === "string") {
    return headerToken;
  }
  return undefined;
}

async function ensureAuthorized(
  request: FastifyRequest,
  reply: FastifyReply,
  expectedToken?: string
) {
  if (!expectedToken) {
    return;
  }
  const token = extractToken(request);
  if (!token) {
    throw reply.server.httpErrors.unauthorized("Missing service token");
  }
  if (token !== expectedToken) {
    throw reply.server.httpErrors.forbidden("Invalid service token");
  }
}

const serviceAuthPlugin = fp(async function serviceAuthPlugin(
  fastify: FastifyInstance
) {
  const config = loadConfig();

  fastify.decorate(
    "verifyServiceRequest",
    async (request: FastifyRequest, reply: FastifyReply) => {
      await ensureAuthorized(request, reply, config.SERVICE_AUTH_TOKEN);
    }
  );

  fastify.addHook("onRequest", async (request, reply) => {
    if (!request.url.startsWith("/internal")) {
      return;
    }
    await ensureAuthorized(request, reply, config.SERVICE_AUTH_TOKEN);
  });
});

declare module "fastify" {
  interface FastifyInstance {
    verifyServiceRequest(request: FastifyRequest, reply: FastifyReply): Promise<void>;
  }
}

export default serviceAuthPlugin;
