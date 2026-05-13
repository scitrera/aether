package tracing

import (
	"context"
	"os"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// LogBridge is a zerolog.Hook that exports every log event to the
// OpenTelemetry collector via OTLP HTTP with trace context correlation.
// When OTEL_EXPORTER_OTLP_ENDPOINT is unset the bridge is a no-op.
type LogBridge struct {
	provider *sdklog.LoggerProvider
}

// NewLogBridge creates an OTLP HTTP log exporter and returns a *LogBridge
// that satisfies zerolog.Hook. If OTEL_EXPORTER_OTLP_ENDPOINT is not set,
// returns a no-op bridge (nil provider, Run returns immediately).
func NewLogBridge(ctx context.Context, serviceName string) (*LogBridge, error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		return &LogBridge{}, nil
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		return nil, err
	}

	exporter, err := otlploghttp.New(ctx)
	if err != nil {
		return nil, err
	}

	provider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
		sdklog.WithResource(res),
	)

	return &LogBridge{provider: provider}, nil
}

// Run implements zerolog.Hook. It maps the zerolog event to an OTel log
// record and emits it via the OTLP exporter. The OTel SDK automatically
// extracts trace_id and span_id from the context stored in the event,
// providing trace-to-log correlation in SigNoz.
func (b *LogBridge) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	if b.provider == nil {
		return
	}

	var sev otellog.Severity
	switch level {
	case zerolog.DebugLevel:
		sev = otellog.SeverityDebug
	case zerolog.InfoLevel:
		sev = otellog.SeverityInfo
	case zerolog.WarnLevel:
		sev = otellog.SeverityWarn
	case zerolog.ErrorLevel, zerolog.FatalLevel, zerolog.PanicLevel:
		sev = otellog.SeverityError
	default:
		sev = otellog.SeverityInfo
	}

	now := time.Now()
	record := otellog.Record{}
	record.SetTimestamp(now)
	record.SetObservedTimestamp(now)
	record.SetSeverity(sev)
	record.SetSeverityText(level.String())
	record.SetBody(otellog.StringValue(msg))

	logger := b.provider.Logger("github.com/scitrera/aether/gateway")
	logger.Emit(e.GetCtx(), record)
}

// Shutdown flushes pending log records and shuts down the exporter.
func (b *LogBridge) Shutdown(ctx context.Context) error {
	if b.provider == nil {
		return nil
	}
	return b.provider.Shutdown(ctx)
}
