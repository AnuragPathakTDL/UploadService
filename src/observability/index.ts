import { NodeSDK, metrics } from "@opentelemetry/sdk-node";
import { getNodeAutoInstrumentations } from "@opentelemetry/auto-instrumentations-node";
import { Resource } from "@opentelemetry/resources";
import { SemanticResourceAttributes } from "@opentelemetry/semantic-conventions";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { OTLPMetricExporter } from "@opentelemetry/exporter-metrics-otlp-http";

export type ObservabilityConfig = {
  serviceName: string;
  serviceVersion?: string;
  tracesEndpoint?: string;
  metricsEndpoint?: string;
  metricsExportIntervalMillis: number;
};

let sdk: NodeSDK | null = null;

function buildResource(config: ObservabilityConfig) {
  return new Resource({
    [SemanticResourceAttributes.SERVICE_NAME]: config.serviceName,
    [SemanticResourceAttributes.SERVICE_VERSION]:
      config.serviceVersion ?? process.env.npm_package_version ?? "0.0.0",
    [SemanticResourceAttributes.DEPLOYMENT_ENVIRONMENT]:
      process.env.NODE_ENV ?? "development",
  });
}

export async function startObservability(config: ObservabilityConfig) {
  if (sdk) {
    return sdk;
  }

  const instrumentations = getNodeAutoInstrumentations({
    "@opentelemetry/instrumentation-http": {
      ignoreOutgoingRequestHook: (request) =>
        typeof request?.path === "string" && request.path.includes("/health"),
    },
  });

  const traceExporter = config.tracesEndpoint
    ? new OTLPTraceExporter({ url: config.tracesEndpoint })
    : undefined;

  const metricReader = config.metricsEndpoint
    ? new metrics.PeriodicExportingMetricReader({
        exporter: new OTLPMetricExporter({ url: config.metricsEndpoint }),
        exportIntervalMillis: config.metricsExportIntervalMillis,
      })
    : undefined;

  sdk = new NodeSDK({
    resource: buildResource(config),
    traceExporter,
    metricReader,
    instrumentations,
  });

  await sdk.start();
  return sdk;
}

export async function shutdownObservability() {
  if (!sdk) {
    return;
  }
  await sdk.shutdown();
  sdk = null;
}
