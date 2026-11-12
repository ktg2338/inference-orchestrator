package eventhub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"

	"scoring/internal/models"
)


type MessageHandler func(ctx context.Context, msg *models.ScoringMessage) error

type Consumer struct {
	client  *azeventhubs.ConsumerClient
	handler MessageHandler
}

func NewConsumer(connStr, eventHubName string, handler MessageHandler) (*Consumer, error) {
	client, err := azeventhubs.NewConsumerClientFromConnectionString(
		connStr,
		eventHubName,
		azeventhubs.DefaultConsumerGroup,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("event hub consumer 생성 실패: %w", err)
	}

	return &Consumer{
		client:  client,
		handler: handler,
	}, nil
}

func (c *Consumer) StartListening(ctx context.Context) error {
	// Event Hub의 모든 파티션 ID 가져오기
	props, err := c.client.GetEventHubProperties(ctx, nil)
	if err != nil {
		return fmt.Errorf("Event Hub 속성 가져오기 실패: %w", err)
	}

	log.Printf("Event Hub 메시지 수신 시작... (파티션 수: %d)", len(props.PartitionIDs))

	// 각 파티션마다 goroutine으로 메시지 수신
	for _, partitionID := range props.PartitionIDs {
		go c.listenPartition(ctx, partitionID)
	}

	// 메인 goroutine은 계속 실행
	select {}
}

func (c *Consumer) listenPartition(ctx context.Context, partitionID string) {
	log.Printf("파티션 %s 리스닝 시작", partitionID)

	partitionClient, err := c.client.NewPartitionClient(partitionID, nil)
	if err != nil {
		log.Printf("파티션 %s 클라이언트 생성 실패: %v", partitionID, err)
		return
	}
	defer partitionClient.Close(ctx)

	for {
		// 최대 10개 이벤트, 60초 대기
		receiveCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		events, err := partitionClient.ReceiveEvents(receiveCtx, 10, nil)
		cancel()

		if err != nil {
			// context deadline exceeded는 메시지가 없을 때의 정상 동작이므로 로그 출력 안 함
			if !errors.Is(err, context.DeadlineExceeded) {
				log.Printf("파티션 %s 메시지 수신 오류: %v", partitionID, err)
			}
			time.Sleep(5 * time.Second)
			continue
		}

		if len(events) > 0 {
			log.Printf("파티션 %s에서 %d개 메시지 수신됨", partitionID, len(events))
		}

		for _, event := range events {
			log.Printf("[파티션 %s] 메시지 수신: %s", partitionID, string(event.Body))

			msg, err := parseMessage(event.Body)
			if err != nil {
				log.Printf("[파티션 %s] 메시지 파싱 오류: %v, 원본: %s", partitionID, err, string(event.Body))
				continue
			}

			// 비동기로 처리
			go func(m *models.ScoringMessage) {
				if err := c.handler(ctx, m); err != nil {
					log.Printf("[파티션 %s] 메시지 처리 오류 (namespace=%s, job=%s): %v", partitionID, m.Namespace, m.JobName, err)
				}
			}(msg)
		}
	}
}

func parseMessage(data []byte) (*models.ScoringMessage, error) {
	msg := &models.ScoringMessage{}
	if err := json.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("JSON 파싱 실패: %w", err)
	}

	// 필수 필드 검증
	if msg.SubmissionURL == "" {
		return nil, fmt.Errorf("submission_url이 비어있습니다")
	}
	if msg.TaskURL == "" {
		return nil, fmt.Errorf("task_url이 비어있습니다")
	}
	if msg.JobName == "" {
		return nil, fmt.Errorf("job_name이 비어있습니다")
	}
	if msg.Namespace == "" {
		return nil, fmt.Errorf("namespace가 비어있습니다")
	}
	if msg.JobYamlURL == "" {
		return nil, fmt.Errorf("job_yaml_url이 비어있습니다")
	}

	return msg, nil
}

func (c *Consumer) Close(ctx context.Context) error {
	return c.client.Close(ctx)
}
