package storage

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

// DownloadFromURL은 URL(SAS URL 포함)에서 파일을 다운로드합니다
func DownloadFromURL(ctx context.Context, url, destPath string) error {
	// HTTP GET 요청 생성
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("HTTP 요청 생성 실패 (url=%s): %w", url, err)
	}

	// HTTP 요청 실행
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("다운로드 실패 (url=%s): %w", url, err)
	}
	defer resp.Body.Close()

	// HTTP 상태 코드 확인
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("다운로드 실패 (url=%s, status=%d)", url, resp.StatusCode)
	}

	// 파일 생성
	file, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("파일 생성 실패 (path=%s): %w", destPath, err)
	}
	defer file.Close()

	// 다운로드한 내용을 파일에 쓰기
	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return fmt.Errorf("파일 쓰기 실패 (path=%s): %w", destPath, err)
	}

	log.Printf("다운로드 완료: %s -> %s", url, destPath)
	return nil
}
