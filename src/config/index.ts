import dotenv from "dotenv";
import { z } from "zod";

dotenv.config();

const envSchema = z.object({
  NODE_ENV: z
    .enum(["development", "test", "production"])
    .default("development"),
  HTTP_HOST: z.string().default("0.0.0.0"),
  HTTP_PORT: z.coerce.number().int().positive().default(5000),
  HTTP_BODY_LIMIT: z.coerce
    .number()
    .int()
    .positive()
    .default(25 * 1024 * 1024),
  LOG_LEVEL: z
    .enum(["fatal", "error", "warn", "info", "debug", "trace"])
    .default("info"),
  SERVICE_AUTH_TOKEN: z
    .string()
    .optional()
    .transform((value) =>
      value && value.trim().length > 0 ? value : undefined
    ),
  DATABASE_URL: z
    .string()
    .url()
    .default(
      "postgresql://postgres:postgres@postgres:5432/pocketlol_uploads?schema=public"
    ),
  REDIS_URL: z.string().url().default("redis://redis:6379/1"),
  UPLOAD_CONCURRENT_LIMIT: z.coerce.number().int().positive().default(5),
  UPLOAD_DAILY_LIMIT: z.coerce.number().int().positive().default(50),
  GCP_PROJECT_ID: z
    .string()
    .optional()
    .transform((value) =>
      value && value.trim().length > 0 ? value : undefined
    ),
  GCP_SERVICE_ACCOUNT_KEY: z
    .string()
    .optional()
    .transform((value) =>
      value && value.trim().length > 0 ? value : undefined
    ),
  UPLOAD_BUCKET: z.string().default("pocketlol-uploads"),
  CDN_UPLOAD_BASE_URL: z.string().url().default("https://upload.cdn.pocketlol"),
  SIGNED_UPLOAD_TTL_SECONDS: z.coerce.number().int().positive().default(600),
  DEFAULT_TENANT_ID: z.string().min(1).default("pocketlol"),
  DEFAULT_INGEST_REGION: z.string().min(1).default("us-central1"),
  PUBSUB_PROJECT_ID: z
    .string()
    .optional()
    .transform((value) =>
      value && value.trim().length > 0 ? value : undefined
    ),
  MEDIA_UPLOADED_TOPIC: z.string().default("media.uploaded"),
  MEDIA_READY_FOR_STREAM_TOPIC: z.string().default("media.ready-for-stream"),
  PREVIEW_GENERATION_TOPIC: z.string().default("media.preview.requested"),
  MEDIA_PROCESSED_TOPIC: z.string().default("media.processed"),
  CLEANUP_INTERVAL_SECONDS: z.coerce.number().int().positive().default(300),
  OTEL_TRACES_ENDPOINT: z
    .string()
    .url()
    .optional()
    .transform((value) =>
      value && value.trim().length > 0 ? value : undefined
    ),
  OTEL_METRICS_ENDPOINT: z
    .string()
    .url()
    .optional()
    .transform((value) =>
      value && value.trim().length > 0 ? value : undefined
    ),
  OTEL_METRICS_INTERVAL_MS: z.coerce.number().int().positive().default(15000),
  ENABLE_AUDIT_EVENTS: z
    .string()
    .optional()
    .transform((value) => value === "true"),
  AUDIT_EVENT_SINK_URL: z
    .string()
    .url()
    .optional()
    .transform((value) =>
      value && value.trim().length > 0 ? value : undefined
    ),
  AUDIT_EVENT_SINK_TOKEN: z
    .string()
    .optional()
    .transform((value) =>
      value && value.trim().length > 0 ? value : undefined
    ),
  AUDIT_EVENT_SINK_TIMEOUT_MS: z.coerce.number().int().positive().optional(),
});

export type Env = z.infer<typeof envSchema>;

let cachedConfig: Env | null = null;

export function loadConfig(): Env {
  if (cachedConfig) {
    return cachedConfig;
  }
  const parsed = envSchema.safeParse(process.env);
  if (!parsed.success) {
    const message = parsed.error.issues
      .map((issue) => `${issue.path.join(".")}: ${issue.message}`)
      .join("; ");
    throw new Error(`UploadService configuration invalid: ${message}`);
  }
  cachedConfig = parsed.data;
  return cachedConfig;
}

export function resetConfigCache() {
  cachedConfig = null;
}
