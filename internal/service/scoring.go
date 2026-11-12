package service

import (
	"context"
	"fmt"
	"log"
	"os"

	batchv1 "k8s.io/api/batch/v1"
	"sigs.k8s.io/yaml"

	"scoring/internal/k8s"
	"scoring/internal/models"
	"scoring/internal/storage"
)

type ScoringService struct {
	jobManager *k8s.JobManager
}

func NewScoringService() (*ScoringService, error) {
	jobManager, err := k8s.NewJobManager()
	if err != nil {
		return nil, fmt.Errorf("job manager 생성 실패: %w", err)
	}

	return &ScoringService{
		jobManager: jobManager,
	}, nil
}

func (s *ScoringService) ProcessScoring(ctx context.Context, msg *models.ScoringMessage) error {
	log.Printf("[%s/%s] 채점 시작", msg.Namespace, msg.JobName)

	// 1. job.yaml 다운로드 및 파싱
	jobYamlPath := fmt.Sprintf("/tmp/%s_job.yaml", msg.JobName)
	if err := storage.DownloadFromURL(ctx, msg.JobYamlURL, jobYamlPath); err != nil {
		return fmt.Errorf("job.yaml 다운로드 실패 (namespace=%s, job=%s): %w", msg.Namespace, msg.JobName, err)
	}
	defer os.Remove(jobYamlPath)

	// job.yaml 파일 읽기 및 파싱
	content, err := os.ReadFile(jobYamlPath)
	if err != nil {
		return fmt.Errorf("job.yaml 읽기 실패 (namespace=%s, job=%s): %w", msg.Namespace, msg.JobName, err)
	}

	jobTemplate := &batchv1.Job{}
	if err := yaml.Unmarshal(content, jobTemplate); err != nil {
		return fmt.Errorf("job.yaml 파싱 실패 (namespace=%s, job=%s): %w", msg.Namespace, msg.JobName, err)
	}
	log.Printf("[%s/%s] job.yaml 파싱 완료", msg.Namespace, msg.JobName)

	// 2. Job 생성 (InitContainer가 파일 다운로드/압축해제 수행)
	if err := s.jobManager.CreateJob(ctx, msg, jobTemplate); err != nil {
		return fmt.Errorf("job 생성 실패 (namespace=%s, job=%s): %w", msg.Namespace, msg.JobName, err)
	}

	// TTL로 자동 삭제되므로 수동 삭제는 선택적
	// defer func() {
	// 	if delErr := s.jobManager.DeleteJob(ctx, msg.JobName, msg.Namespace); delErr != nil {
	// 		log.Printf("[%s/%s] Job 삭제 실패 (defer에서): %v", msg.Namespace, msg.JobName, delErr)
	// 	}
	// }()

	// 3. Job이 생성한 Pod 이름 찾기
	podName, err := s.jobManager.GetJobPodName(ctx, msg.JobName, msg.Namespace)
	if err != nil {
		return fmt.Errorf("job pod 찾기 실패 (namespace=%s, job=%s): %w", msg.Namespace, msg.JobName, err)
	}
	log.Printf("[%s/%s] Job의 Pod: %s", msg.Namespace, msg.JobName, podName)

	// 4. Pod Ready 대기 (InitContainer가 파일 다운로드/압축해제 완료)
	if err := s.jobManager.WaitForPodReady(ctx, podName, msg.Namespace); err != nil {
		return fmt.Errorf("pod Ready 대기 실패 (namespace=%s, pod=%s): %w", msg.Namespace, podName, err)
	}

	// 5. 채점 실행
	output, err := s.executeScoring(ctx, podName, msg.Namespace)
	if err != nil {
		return fmt.Errorf("채점 실행 실패 (namespace=%s, job=%s): %w", msg.Namespace, msg.JobName, err)
	}

	log.Printf("[%s/%s] 채점 실행 완료 (출력 길이: %d bytes)", msg.Namespace, msg.JobName, len(output))

	// 6. Job 완료 대기 (선택적 - 채점 스크립트가 종료되면 Job도 완료됨)
	log.Printf("[%s/%s] Job 완료 대기 중...", msg.Namespace, msg.JobName)
	if err := s.jobManager.WaitForJobComplete(ctx, msg.JobName, msg.Namespace); err != nil {
		log.Printf("[%s/%s] Job 완료 대기 실패: %v", msg.Namespace, msg.JobName, err)
		// 채점은 이미 완료되었으므로 에러는 로그만 남기고 계속 진행
	}

	return nil
}

func (s *ScoringService) executeScoring(ctx context.Context, podName, namespace string) (string, error) {
	cmd := []string{
		"python3",
		"/aif/score_wrapper.py",
	}

	return s.jobManager.ExecInPod(ctx, podName, namespace, cmd)
}
