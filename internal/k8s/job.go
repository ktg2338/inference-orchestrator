package k8s

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/kubectl/pkg/scheme"

	"scoring/internal/models"
)

type JobManager struct {
	clientset *kubernetes.Clientset
	config    *rest.Config
}

func NewJobManager() (*JobManager, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("k8s config 생성 실패: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("k8s 클라이언트 생성 실패: %w", err)
	}

	return &JobManager{
		clientset: clientset,
		config:    config,
	}, nil
}

func (m *JobManager) CreateJob(ctx context.Context, msg *models.ScoringMessage, jobTemplate *batchv1.Job) error {
	log.Printf("[%s/%s] Job 생성 시작 (job.yaml 기반)", msg.Namespace, msg.JobName)

	// 1. 메타데이터 교체
	jobTemplate.ObjectMeta.Name = msg.JobName
	jobTemplate.ObjectMeta.Namespace = msg.Namespace

	// 2. Pod 템플릿의 환경변수 교체/추가 (메인 컨테이너)
	if len(jobTemplate.Spec.Template.Spec.Containers) > 0 {
		mainContainer := &jobTemplate.Spec.Template.Spec.Containers[0]

		// 기존 환경변수를 맵으로 변환
		envMap := make(map[string]string)
		for _, env := range mainContainer.Env {
			envMap[env.Name] = env.Value
		}

		// ScoringMessage의 값으로 업데이트
		envMap["SUBMISSION_ID_PUBLIC"] = msg.SubmissionIDPublic
		envMap["SUBMISSION_ID_TOTAL"] = msg.SubmissionIDTotal
		envMap["API_ADDRESS"] = msg.APIAddress

		// 맵을 다시 []EnvVar로 변환
		newEnvVars := make([]corev1.EnvVar, 0, len(envMap))
		for k, v := range envMap {
			newEnvVars = append(newEnvVars, corev1.EnvVar{Name: k, Value: v})
		}
		mainContainer.Env = newEnvVars

		// 컨테이너 이름도 교체
		mainContainer.Name = msg.JobName
	}

	// 2-1. InitContainer의 환경변수도 교체
	for i := range jobTemplate.Spec.Template.Spec.InitContainers {
		initContainer := &jobTemplate.Spec.Template.Spec.InitContainers[i]

		// 기존 환경변수를 맵으로 변환
		envMap := make(map[string]string)
		for _, env := range initContainer.Env {
			envMap[env.Name] = env.Value
		}

		// ScoringMessage의 값으로 업데이트
		envMap["SUBMISSION_URL"] = msg.SubmissionURL
		envMap["TASK_URL"] = msg.TaskURL

		// 맵을 다시 []EnvVar로 변환
		newEnvVars := make([]corev1.EnvVar, 0, len(envMap))
		for k, v := range envMap {
			newEnvVars = append(newEnvVars, corev1.EnvVar{Name: k, Value: v})
		}
		initContainer.Env = newEnvVars
	}

	// 3. initContainer의 이름도 필요시 교체
	for i := range jobTemplate.Spec.Template.Spec.InitContainers {
		initContainer := &jobTemplate.Spec.Template.Spec.InitContainers[i]
		// placeholder 교체 (예: jobname-replacement)
		if strings.Contains(initContainer.Name, "replacement") {
			initContainer.Name = strings.ReplaceAll(initContainer.Name, "replacement", msg.JobName)
		}
	}

	// 4. 이미지 교체 (ScoringMessage.ImageName이 있으면 우선 사용)
	if msg.ImageName != "" {
		newImage := fmt.Sprintf("aifprodacr.azurecr.io/aifactory-evaluation:%s", msg.ImageName)
		log.Printf("[%s/%s] ScoringMessage.ImageName으로 이미지 교체: %s", msg.Namespace, msg.JobName, newImage)

		// 모든 컨테이너와 initContainer의 이미지 교체
		for i := range jobTemplate.Spec.Template.Spec.Containers {
			jobTemplate.Spec.Template.Spec.Containers[i].Image = newImage
		}
		for i := range jobTemplate.Spec.Template.Spec.InitContainers {
			// 데이터 이미지는 교체하지 않음 (copy-data-from-image 등)
			if !strings.Contains(jobTemplate.Spec.Template.Spec.InitContainers[i].Name, "copy-data") {
				jobTemplate.Spec.Template.Spec.InitContainers[i].Image = newImage
			}
		}
	}

	// 5. Job 생성
	_, err := m.clientset.BatchV1().Jobs(msg.Namespace).Create(ctx, jobTemplate, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("Job 생성 API 호출 실패 (namespace=%s, job=%s): %w", msg.Namespace, msg.JobName, err)
	}

	log.Printf("[%s/%s] Job 생성 완료", msg.Namespace, msg.JobName)
	return nil
}

// GetJobPodName은 Job이 생성한 Pod의 이름을 찾습니다
func (m *JobManager) GetJobPodName(ctx context.Context, jobName, namespace string) (string, error) {
	// Job의 Pod를 찾기 위해 label selector 사용
	labelSelector := fmt.Sprintf("job-name=%s", jobName)

	timeout := time.After(5 * time.Minute)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return "", fmt.Errorf("Job Pod 찾기 타임아웃 (namespace=%s, job=%s)", namespace, jobName)
		case <-ticker.C:
			pods, err := m.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
				LabelSelector: labelSelector,
			})
			if err != nil {
				return "", fmt.Errorf("Pod 목록 조회 실패 (namespace=%s, job=%s): %w", namespace, jobName, err)
			}

			if len(pods.Items) > 0 {
				podName := pods.Items[0].Name
				log.Printf("[%s/%s] Job의 Pod 찾음: %s", namespace, jobName, podName)
				return podName, nil
			}
		}
	}
}

// WaitForPodReady는 Pod가 Ready 상태가 될 때까지 대기합니다
func (m *JobManager) WaitForPodReady(ctx context.Context, podName, namespace string) error {
	timeout := time.After(20 * time.Minute)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("Pod Ready 타임아웃 (namespace=%s, pod=%s)", namespace, podName)
		case <-ticker.C:
			pod, err := m.clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("Pod 상태 조회 실패 (namespace=%s, pod=%s): %w", namespace, podName, err)
			}

			if pod.Status.Phase == corev1.PodRunning {
				for _, condition := range pod.Status.Conditions {
					if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
						log.Printf("[%s/%s] Pod Ready", namespace, podName)
						return nil
					}
				}
			}
		}
	}
}

// WaitForJobComplete는 Job이 완료될 때까지 대기합니다
func (m *JobManager) WaitForJobComplete(ctx context.Context, jobName, namespace string) error {
	timeout := time.After(30 * time.Minute) // 채점은 시간이 오래 걸릴 수 있음
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("Job 완료 타임아웃 (namespace=%s, job=%s)", namespace, jobName)
		case <-ticker.C:
			job, err := m.clientset.BatchV1().Jobs(namespace).Get(ctx, jobName, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("Job 상태 조회 실패 (namespace=%s, job=%s): %w", namespace, jobName, err)
			}

			// Job이 성공적으로 완료됨
			if job.Status.Succeeded > 0 {
				log.Printf("[%s/%s] Job 성공 완료", namespace, jobName)
				return nil
			}

			// Job이 실패함 (backoffLimit 초과)
			if job.Status.Failed > 0 && job.Status.Active == 0 {
				return fmt.Errorf("Job 실패 (namespace=%s, job=%s, failed=%d)", namespace, jobName, job.Status.Failed)
			}

			// 아직 실행 중
			log.Printf("[%s/%s] Job 실행 중 (active=%d, succeeded=%d, failed=%d)",
				namespace, jobName, job.Status.Active, job.Status.Succeeded, job.Status.Failed)
		}
	}
}

func (m *JobManager) DeleteJob(ctx context.Context, jobName, namespace string) error {
	log.Printf("[%s/%s] Job 삭제 중...", namespace, jobName)

	// Job을 삭제하면 연관된 Pod도 함께 삭제됨 (propagationPolicy 기본값)
	err := m.clientset.BatchV1().Jobs(namespace).Delete(ctx, jobName, metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("Job 삭제 실패 (namespace=%s, job=%s): %w", namespace, jobName, err)
	}

	log.Printf("[%s/%s] Job 삭제 완료", namespace, jobName)
	return nil
}

// ExecInPod executes a command in a Pod
func (m *JobManager) ExecInPod(ctx context.Context, podName, namespace string, cmd []string) (string, error) {
	req := m.clientset.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Command: cmd,
			Stdin:   false,
			Stdout:  true,
			Stderr:  true,
			TTY:     false,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(m.config, "POST", req.URL())
	if err != nil {
		return "", fmt.Errorf("SPDY executor 생성 실패 (namespace=%s, pod=%s): %w", namespace, podName, err)
	}

	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	if err != nil {
		return stdout.String(), fmt.Errorf("명령 실행 실패 (namespace=%s, pod=%s, cmd=%v): %w, stderr: %s", namespace, podName, cmd, err, stderr.String())
	}

	if stderr.Len() > 0 {
		log.Printf("[%s/%s] 명령 실행 stderr: %s", namespace, podName, stderr.String())
	}

	return stdout.String(), nil
}

// CopyFilesToPod copies files from local directory to Pod using tar
func (m *JobManager) CopyFilesToPod(ctx context.Context, podName, namespace string, tarBuffer *bytes.Buffer, destPath string) error {
	log.Printf("[%s/%s] 파일 복사 시작 -> %s", namespace, podName, destPath)

	// Pod에 tar 아카이브 전송 및 압축 해제
	req := m.clientset.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Command: []string{"tar", "xf", "-", "-C", destPath},
			Stdin:   true,
			Stdout:  true,
			Stderr:  true,
			TTY:     false,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(m.config, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("executor 생성 실패 (namespace=%s, pod=%s): %w", namespace, podName, err)
	}

	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  tarBuffer,
		Stdout: &stdout,
		Stderr: &stderr,
	})

	if err != nil {
		return fmt.Errorf("파일 복사 실패 (namespace=%s, pod=%s, dest=%s): %w, stderr: %s", namespace, podName, destPath, err, stderr.String())
	}

	if stderr.Len() > 0 {
		log.Printf("[%s/%s] 복사 stderr: %s", namespace, podName, stderr.String())
	}

	log.Printf("[%s/%s] 파일 복사 완료 -> %s", namespace, podName, destPath)
	return nil
}
