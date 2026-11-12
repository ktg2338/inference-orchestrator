package main

import (
	"context"
	"log"
	"os"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"

	eventHub "scoring/internal/eventHub"
	"scoring/internal/service"
	"scoring/internal/telemetry"
)

func main() {
	// Application Insights telemetry 종료 시 flush
	defer telemetry.Flush()

	// 환경 변수에서 설정 읽기
	eventHubConnStr := os.Getenv("EVENT_HUB_CONN_STR")
	eventHubName := os.Getenv("EVENT_HUB_NAME")

	// 필수 환경 변수 검증
	if eventHubConnStr == "" {
		log.Fatalf("환경 변수 누락: EVENT_HUB_CONN_STR이 설정되지 않았습니다")
	}
	if eventHubName == "" {
		log.Fatalf("환경 변수 누락: EVENT_HUB_NAME이 설정되지 않았습니다")
	}

	log.Printf("서비스 초기화 시작 (eventHub=%s)", eventHubName)

	// ScoringService 초기화 (SAS URL만 사용하므로 Connection String 불필요)
	scoringService, err := service.NewScoringService()
	if err != nil {
		log.Fatalf("서비스 초기화 실패: %v", err)
	}

	// Event Hub Consumer 초기화
	consumer, err := eventHub.NewConsumer(eventHubConnStr, eventHubName, scoringService.ProcessScoring)
	if err != nil {
		log.Fatalf("Event Hub consumer 초기화 실패: %v", err)
	}

	log.Printf("서비스 초기화 완료")

	// Gin 웹 서버 (헬스체크용)
	r := gin.New()

	// OpenTelemetry 자동 계측 미들웨어 추가
	r.Use(otelgin.Middleware("scoring"))

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "healthy"})
	})

	// 백그라운드에서 Event Hub 리스닝 시작
	ctx := context.Background()
	go func() {
		if err := consumer.StartListening(ctx); err != nil {
			log.Fatalf("Event Hub 리스닝 오류: %v", err)
		}
	}()

	// 웹 서버 시작
	log.Println("서버 시작: :8080")
	r.Run(":8080")
}
