export interface AuditEvent {
  type: string;
  uploadId?: string;
  adminId?: string;
  contentId?: string;
  storageKey?: string;
  manifestUrl?: string;
  defaultThumbnailUrl?: string;
  correlationId?: string;
  status?: string;
  metadata?: Record<string, unknown>;
  occurredAt?: string;
  source?: string;
}
