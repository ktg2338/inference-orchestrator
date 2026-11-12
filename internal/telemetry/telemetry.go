package telemetry

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	"github.com/microsoft/ApplicationInsights-Go/appinsights"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

var (
	// Application Insights 클라이언트
	aiClient appinsights.TelemetryClient
	// OpenTelemetry tracer provider
	tracerProvider *sdktrace.TracerProvider
	// telemetry 활성화 여부
	enabled bool
)

// init 함수에서 자동으로 초기화
func init() {
	connectionString := os.Getenv("AZURE_MONITOR_CONNECTION_STRING")

	// Connection String이 없으면 telemetry 비활성화
	if connectionString == "" {
		log.Println("[Telemetry] Application Insights 비활성화 (AZURE_MONITOR_CONNECTION_STRING 미설정)")
		enabled = false
		return
	}

	// Connection String에서 Instrumentation Key 추출
	instrumentationKey := extractInstrumentationKey(connectionString)
	if instrumentationKey == "" {
		log.Println("[Telemetry] Connection String이 올바르지 않습니다")
		enabled = false
		return
	}

	// Application Insights 클라이언트 초기화
	config := appinsights.NewTelemetryConfiguration(instrumentationKey)
	config.MaxBatchInterval = 2 * time.Second
	config.MaxBatchSize = 100
	aiClient = appinsights.NewTelemetryClientFromConfig(config)

	// OpenTelemetry 리소스 설정
	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String("scoring"),
		),
	)
	if err != nil {
		log.Printf("[Telemetry] OpenTelemetry 리소스 생성 실패: %v", err)
		enabled = false
		return
	}

	// Application Insights로 전송하는 커스텀 SpanProcessor
	aiProcessor := NewAppInsightsSpanProcessor(aiClient)

	// TracerProvider 초기화
	tracerProvider = sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(aiProcessor),
	)

	// 글로벌 TracerProvider 설정
	otel.SetTracerProvider(tracerProvider)

	enabled = true
	log.Println("[Telemetry] OpenTelemetry + Application Insights 자동 초기화 완료")

	// 초기화 이벤트 전송
	event := appinsights.NewEventTelemetry("ServiceStarted")
	event.Properties["service"] = "scoring"
	aiClient.Track(event)
}

// extractInstrumentationKey는 Connection String에서 Instrumentation Key를 추출합니다
func extractInstrumentationKey(connectionString string) string {
	parts := strings.Split(connectionString, ";")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "InstrumentationKey=") {
			return strings.TrimPrefix(part, "InstrumentationKey=")
		}
	}
	return ""
}

// Flush는 종료 전 모든 telemetry를 전송합니다
func Flush() {
	if !enabled {
		return
	}

	log.Println("[Telemetry] Flushing telemetry data...")

	// OpenTelemetry 종료
	if tracerProvider != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tracerProvider.Shutdown(ctx); err != nil {
			log.Printf("[Telemetry] TracerProvider shutdown error: %v", err)
		}
	}

	// Application Insights 전송
	select {
	case <-aiClient.Channel().Close(10 * time.Second):
		log.Println("[Telemetry] 전송 완료")
	case <-time.After(15 * time.Second):
		log.Println("[Telemetry] 전송 타임아웃")
	}
}

// AppInsightsSpanProcessor는 OpenTelemetry span을 Application Insights로 전송합니다
type AppInsightsSpanProcessor struct {
	client appinsights.TelemetryClient
}

// NewAppInsightsSpanProcessor creates a new span processor
func NewAppInsightsSpanProcessor(client appinsights.TelemetryClient) *AppInsightsSpanProcessor {
	return &AppInsightsSpanProcessor{client: client}
}

// OnStart는 span 시작 시 호출됩니다
func (p *AppInsightsSpanProcessor) OnStart(parent context.Context, s sdktrace.ReadWriteSpan) {
	// 시작 시에는 아무 것도 하지 않음
}

// OnEnd는 span 종료 시 호출됩니다
func (p *AppInsightsSpanProcessor) OnEnd(s sdktrace.ReadOnlySpan) {
	if !enabled {
		return
	}

	// span을 Application Insights request로 변환
	duration := s.EndTime().Sub(s.StartTime())

	request := appinsights.NewRequestTelemetry(
		"POST",
		s.Name(),
		duration,
		"200", // OpenTelemetry span은 성공으로 간주
	)
	request.Success = s.Status().Code != codes.Error

	// span attributes를 properties로 변환
	for _, attr := range s.Attributes() {
		request.Properties[string(attr.Key)] = attr.Value.AsString()
	}

	p.client.Track(request)
}

// Shutdown는 processor를 종료합니다
func (p *AppInsightsSpanProcessor) Shutdown(ctx context.Context) error {
	return nil
}

// ForceFlush는 강제로 flush합니다
func (p *AppInsightsSpanProcessor) ForceFlush(ctx context.Context) error {
	return nil
}
