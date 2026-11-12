# Inference Orchestrator

Azure Event Hub에서 스코어링 요청을 수신하고, Kubernetes Job을 동적으로 생성하여 ML 모델 추론 워크로드의 전체 생명주기를 관리하는 이벤트 기반 오케스트레이터입니다.

## 아키텍처

```
Azure Event Hub  →  Orchestrator  →  Kubernetes Job  →  Scoring Script
                        │
                   SAS URL에서
                   job.yaml 다운로드
                        │
                   환경변수 주입 후
                   K8s Job 생성
                        │
                   Pod Ready 대기
                        │
                   스코어링 스크립트 실행
                   (python3 score_wrapper.py)
                        │
                   완료 모니터링
```

## 기술 스택

| 구성요소 | 기술 |
|----------|------|
| 언어 | Go 1.25 |
| 웹 프레임워크 | Gin |
| 오케스트레이션 | Kubernetes (AKS) |
| 메시징 | Azure Event Hub |
| 스토리지 | Azure Blob Storage |
| 텔레메트리 | OpenTelemetry + Application Insights |
| CI/CD | GitHub Actions |

## 프로젝트 구조

```
inference-orchestrator/
├── cmd/server/main.go            # 진입점
├── internal/
│   ├── eventHub/consumer.go      # Event Hub 메시지 컨슈머
│   ├── service/scoring.go        # 스코어링 오케스트레이션 핵심 로직
│   ├── models/message.go         # ScoringMessage 구조체
│   ├── k8s/job.go                # Kubernetes Job 관리
│   ├── storage/downloader.go     # SAS URL HTTP 다운로더
│   └── telemetry/telemetry.go    # OpenTelemetry + App Insights 설정
├── k8s/
│   ├── deployment.yaml           # Deployment 매니페스트
│   ├── service.yaml              # LoadBalancer 서비스
│   ├── rbac.yaml                 # RBAC, 네임스페이스, 쿼터, 네트워크 정책
│   └── hpa.yaml                  # Horizontal Pod Autoscaler
├── Dockerfile                    # 멀티스테이지 Docker 빌드
├── .github/workflows/deploy.yml  # CI/CD 파이프라인
├── scoring_message.json          # 이벤트 메시지 예시
└── send_event.py                 # 테스트 이벤트 전송 스크립트
```

## 동작 방식

1. 외부 시스템이 Azure Event Hub에 `ScoringMessage`를 발행
2. Orchestrator가 Event Hub 파티션에서 메시지를 동시(concurrent) 소비
3. 제공된 SAS URL에서 `job.yaml`을 다운로드하고 K8s Job 템플릿으로 파싱
4. 환경변수를 주입하여 Kubernetes Job 생성
5. InitContainer가 Pod 내에 제출물/태스크 데이터를 다운로드
6. Pod 준비 완료 후 `python3 /aif/score_wrapper.py` 실행
7. Job 완료 모니터링 (30분 타임아웃)
8. TTL에 의해 Job 자동 삭제, Application Insights로 텔레메트리 전송

## 메시지 스키마

```json
{
  "submissionUrl": "https://...",
  "taskUrl": "https://...",
  "jobYamlUrl": "https://...",
  "jobName": "scoring-job-001",
  "namespace": "scoring",
  "submissionIdPublic": "sub-123",
  "submissionIdTotal": "sub-total-123",
  "apiAddress": "https://api.example.com",
  "imageName": "custom-image:latest"
}
```

## 시작하기

### 사전 요구사항

- Go 1.25+
- Kubernetes 클러스터 접근 권한 (AKS)
- Azure Event Hub 및 Blob Storage 자격 증명

### 환경변수

| 변수명 | 설명 | 필수 |
|--------|------|------|
| `EVENT_HUB_CONN_STR` | Azure Event Hub 연결 문자열 | O |
| `EVENT_HUB_NAME` | Event Hub 이름 | O |
| `BLOB_CONN_STR` | Azure Blob Storage 연결 문자열 | O |
| `AZURE_MONITOR_CONNECTION_STRING` | Application Insights 연결 문자열 | X |

### 로컬 개발

```bash
go build -o scoring-server ./cmd/server/main.go

export EVENT_HUB_CONN_STR="Endpoint=sb://..."
export EVENT_HUB_NAME="scoring-hub"
./scoring-server
```

### Docker

```bash
docker build -t scoring-server:latest .
docker run -p 8080:8080 \
  -e EVENT_HUB_CONN_STR="..." \
  -e EVENT_HUB_NAME="..." \
  scoring-server:latest
```

### Kubernetes 배포

```bash
kubectl apply -f k8s/rbac.yaml
kubectl apply -f k8s/hpa.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
```

## API 엔드포인트

| 메서드 | 경로 | 설명 |
|--------|------|------|
| GET | `/health` | 헬스 체크 (K8s liveness/readiness 프로브에서 사용) |

## Kubernetes 리소스

- **HPA**: 1~10개 레플리카, CPU(>70%) 및 메모리(>80%) 기준 오토스케일링
- **ResourceQuota**: CPU 100개, 메모리 200Gi, 최대 Pod 100개
- **NetworkPolicy**: DNS(53) 및 HTTPS(443)로 이그레스 제한
- **RBAC**: Job, Pod, exec, 로그 권한을 가진 ServiceAccount

## CI/CD

GitHub Actions 워크플로우 (`.github/workflows/deploy.yml`)가 `main` 브랜치 푸시 시 실행:

1. Docker 이미지 빌드 후 Azure Container Registry에 푸시
2. Kubernetes 시크릿 업데이트
3. 매니페스트 배포 및 새 이미지 롤아웃
4. 롤아웃 상태, Pod 로그, 서비스 엔드포인트 검증

## 테스트

Event Hub에 테스트 이벤트 전송:

```bash
pip install azure-eventhub
python send_event.py
```
