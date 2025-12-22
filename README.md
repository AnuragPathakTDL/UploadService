# Upload Service

The Upload Service issues signed Google Cloud Storage (GCS) policies for PocketLOL administrators, validates ingest callbacks, and publishes events used by downstream services to finish media processing. It is only reachable through the API Gateway, which enforces administrator JWTs, role verification via User Service, and the shared service token used for inter-service authentication.

## Admin API (exposed via API Gateway)

| Method & Path | Description |
| --- | --- |
| `POST /api/v1/admin/uploads/sign` | Issue a v4 signed POST policy for an admin asset upload. Enforces asset-type validation plus Redis-backed per-admin quotas. |
| `GET /api/v1/admin/uploads/{uploadId}/status` | Return the current processing state, validation metadata, and preview assets for a given upload. |
| `GET /api/v1/admin/uploads/quota` | Report the administrator's active upload count and remaining daily allowance. |

Sample sign request (Gateway → Upload Service):

```http
POST /api/v1/admin/uploads/sign HTTP/1.1
Authorization: Bearer <admin JWT>
X-Correlation-Id: 6508a4c5-8e8d-41de-9fb1-2b4b76b88a8c
Content-Type: application/json

{
  "fileName": "episode-01.mp4",
  "contentType": "video/mp4",
  "sizeBytes": 134217728,
  "assetType": "video",
  "contentId": "4bc4e515-9fd2-476c-bf11-02f171cb4a7e"
}
```

Response (HTTP 200):

```json
{
  "uploadId": "1f0a5b62-0a75-4ee0-9c20-40db583cd564",
  "uploadUrl": "https://storage.googleapis.com/pocketlol-uploads",
  "expiresAt": "2025-12-18T06:15:31.000Z",
  "objectKey": "videos/1702911131123-f1a2b3c4-episode-01.mp4",
  "storageUrl": "gs://pocketlol-uploads/videos/1702911131123-f1a2b3c4-episode-01.mp4",
  "fields": {
    "key": "videos/1702911131123-f1a2b3c4-episode-01.mp4",
    "Content-Type": "video/mp4",
    "success_action_status": "201",
    "x-goog-meta-asset-type": "video",
    "x-goog-meta-content-id": "4bc4e515-9fd2-476c-bf11-02f171cb4a7e",
    "policy": "...",
    "x-goog-signature": "..."
  },
  "cdn": "https://upload.cdn.pocketlol"
}
```

> The API Gateway injects `x-pocketlol-admin-id` and `x-pocketlol-admin-roles` headers (alongside the service token) when proxying to Upload Service.

## Internal Callbacks

| Endpoint | Auth | Purpose |
| --- | --- | --- |
| `POST /internal/uploads/{uploadId}/validation` | `x-service-token` header or `Authorization: Bearer <token>` | Streaming Service confirms ingest validation (dimensions, duration, checksum). |
| `POST /internal/uploads/{uploadId}/processing` | `x-service-token` | Preview/packaging workers signal manifest + thumbnail availability or processing failure. |

Validation payload:

```json
{
  "status": "success",
  "checksum": "8e4f0f6b9d72",
  "durationSeconds": 188.5,
  "width": 1920,
  "height": 1080
}
```

Processing payload (success):

```json
{
  "status": "ready",
  "manifestUrl": "https://streaming.pocketlol/hls/episode-01.m3u8",
  "defaultThumbnailUrl": "https://cdn.pocketlol/thumbnails/episode-01.jpg",
  "bitrateKbps": 4200,
  "previewGeneratedAt": "2025-12-18T06:17:12.000Z"
}
```

## Pub/Sub Topics

- **`media.uploaded`** – emitted after validation succeeds; includes storage URIs, admin/content correlation, and baseline metadata.
- **`media.preview.requested`** – fire-and-forget request for Cloud Functions/FFmpeg preview workers.
- **`media.processed`** – emitted once manifests and preview thumbnails are saved; feeds Content Service catalog reconciliation.

Each topic uses Pub/Sub defaults (60 s ack deadline, exponential backoff, up to five deliveries). Schemas are captured in `APIGW/src/config/openapi.ts`.

## Environment Setup

Copy `.env.example` into `.env` and fill in the secrets that correspond to your environment. The table below lists the most important settings and how to source them:

| Variable | Purpose | Where to obtain |
| --- | --- | --- |
| `SERVICE_AUTH_TOKEN` | Shared S2S token required by API Gateway and internal callbacks. | Secret Manager entry `platform/service-token` provisioned by the Auth team. |
| `DATABASE_URL` | Postgres connection string for ingest metadata and audit trails. | Cloud SQL instance DSN (store in Secret Manager as `uploads/postgres`). |
| `REDIS_URL` | Redis/Memorystore endpoint that backs per-admin quotas. | Memorystore connection string exposed via infra-local outputs. |
| `GCP_SERVICE_ACCOUNT_KEY` | Base64 JSON key with `storage.objects.signUrl` permission used to mint signed policies. | Service account `upload-signing@<project>.iam.gserviceaccount.com` JSON exported from IAM. |
| `UPLOAD_BUCKET` / `CDN_UPLOAD_BASE_URL` | Target bucket and CDN hostname that serve admin uploads. | Created by the infra team; look up in Terraform outputs `upload_bucket_name` and `upload_cdn_domain`. |
| `MEDIA_*_TOPIC` values | Pub/Sub topics for the ingest pipeline. | Use the per-environment topic names published in Terraform outputs (`media_uploaded_topic`, etc.). |
| `ENABLE_AUDIT_EVENTS`, `AUDIT_EVENT_SINK_URL`, `AUDIT_EVENT_SINK_TOKEN` | Controls forwarding of structured admin actions. | Compliance provides the HTTPS sink + token; configure Secret Manager entry `audit/upload-service`. |
| `OTEL_*` | Optional OTLP exporters for traces and metrics. | Point to the shared Observability OTLP collector endpoint. |

## Environment Variables

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| NODE_ENV | no | development | Node runtime environment. |
| HTTP_HOST | no | 0.0.0.0 | Bind address. |
| HTTP_PORT | no | 5000 | HTTP listen port. |
| HTTP_BODY_LIMIT | no | 26214400 | Max inbound payload size (bytes). |
| LOG_LEVEL | no | info | Pino log level. |
| SERVICE_AUTH_TOKEN | yes (prod) | — | Shared secret used by API Gateway and internal workers. |
| DATABASE_URL | yes | — | Postgres connection string (Prisma). |
| REDIS_URL | yes | redis://localhost:6379 | Redis instance for quota tracking. |
| UPLOAD_CONCURRENT_LIMIT | no | 5 | Per-admin concurrent upload cap. |
| UPLOAD_DAILY_LIMIT | no | 50 | Per-admin daily upload cap. |
| UPLOAD_BUCKET | yes | pocketlol-uploads | Target GCS bucket. |
| CDN_UPLOAD_BASE_URL | yes | <https://upload.cdn.pocketlol> | CDN fronting the bucket. |
| SIGNED_UPLOAD_TTL_SECONDS | no | 600 | Lifetime of signed policies (seconds). |
| MEDIA_UPLOADED_TOPIC | no | media.uploaded | Pub/Sub topic for validation success events. |
| MEDIA_PROCESSED_TOPIC | no | media.processed | Pub/Sub topic for processed asset notifications. |
| PREVIEW_GENERATION_TOPIC | no | media.preview.requested | Pub/Sub topic for preview generation requests. |
| ENABLE_AUDIT_EVENTS | no | false | Forward structured audit events when true. |
| AUDIT_EVENT_SINK_URL | required when auditing | — | HTTPS endpoint receiving audit logs. |
| AUDIT_EVENT_SINK_TOKEN | optional | — | Bearer token used for audit sink auth. |
| AUDIT_EVENT_SINK_TIMEOUT_MS | optional | — | Timeout (ms) for audit sink requests. |
| PUBSUB_PROJECT_ID | optional | — | Explicit GCP project for Pub/Sub (overrides ADC). |
| OTEL_TRACES_ENDPOINT | optional | — | OTLP traces endpoint. |
| OTEL_METRICS_ENDPOINT | optional | — | OTLP metrics endpoint. |

## Running Locally

```bash
npm install

export SERVICE_AUTH_TOKEN=local-dev-token
export DATABASE_URL="postgres://localhost:5432/pocketlol"
export REDIS_URL="redis://localhost:6379"
export UPLOAD_BUCKET=pocketlol-uploads

npm run dev
```

To exercise the admin API directly (bypassing the Gateway), include both the service token and admin headers:

```bash
curl -X POST http://localhost:5000/v1/admin/uploads/sign \
  -H "authorization: Bearer ${SERVICE_AUTH_TOKEN}" \
  -H "x-pocketlol-admin-id: c7a20cf3-4be2-4c25-8cf8-4fe2f73d9d3f" \
  -H "x-pocketlol-admin-roles: admin" \
  -H "content-type: application/json" \
  -d '{
    "fileName": "episode-01.mp4",
    "contentType": "video/mp4",
    "sizeBytes": 134217728,
    "assetType": "video"
  }'
```

## Observability & Audit

- Enable OpenTelemetry by setting `ENABLE_TELEMETRY=true` plus OTLP endpoint variables.
- Structured audit events emit for admin intents, validation callbacks, preview requests, and processing outcomes. Configure `ENABLE_AUDIT_EVENTS=true` along with `AUDIT_EVENT_SINK_URL` (and optional token/timeout) to forward them to the shared compliance sink.
