package models

type ScoringMessage struct {
	SubmissionURL      string `json:"submission_url"`
	TaskURL            string `json:"task_url"`
	JobYamlURL         string `json:"job_yaml_url"` // job.yaml SAS URL
	JobName            string `json:"job_name"`  // Pod 대신 Job 이름 사용
	Namespace          string `json:"namespace"`
	SubmissionIDPublic string `json:"submission_id_public"`
	SubmissionIDTotal  string `json:"submission_id_total"`
	APIAddress         string `json:"api_address"`
	ImageName          string `json:"image_name,omitempty"` // 선택적 이미지 이름
}
