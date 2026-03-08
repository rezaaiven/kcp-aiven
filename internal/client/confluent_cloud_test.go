package client

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/confluentinc/kcp/internal/types"
)

func TestIsRetryableStatusCode(t *testing.T) {
	tests := []struct {
		code int
		want bool
	}{
		{200, false},
		{400, false},
		{404, false},
		{429, true},
		{500, true},
		{502, true},
		{503, true},
		{599, true},
		{600, false},
	}
	for _, tt := range tests {
		if got := isRetryableStatusCode(tt.code); got != tt.want {
			t.Errorf("isRetryableStatusCode(%d) = %v, want %v", tt.code, got, tt.want)
		}
	}
}

func TestPageTokenFromNext(t *testing.T) {
	tests := []struct {
		url  string
		want string
	}{
		{"", ""},
		{"https://api.confluent.cloud/org/v2/environments?page_token=abc123", "abc123"},
		{"https://example.com?page_token=xyz&other=1", "xyz"},
		{"https://example.com?other=1", ""},
	}
	for _, tt := range tests {
		if got := pageTokenFromNext(tt.url); got != tt.want {
			t.Errorf("pageTokenFromNext(%q) = %q, want %q", tt.url, got, tt.want)
		}
	}
}

func TestStatusFromError(t *testing.T) {
	if got := statusFromError(nil, nil); got != 0 {
		t.Errorf("statusFromError(nil, nil) = %d, want 0", got)
	}
	resp429 := &http.Response{StatusCode: 429}
	if got := statusFromError(nil, resp429); got != 429 {
		t.Errorf("statusFromError(nil, resp429) = %d, want 429", got)
	}
	err := assertAnError("429 Too Many Requests")
	if got := statusFromError(err, nil); got != 429 {
		t.Errorf("statusFromError(429 err, nil) = %d, want 429", got)
	}
	if got := statusFromError(err, resp429); got != 429 {
		t.Errorf("statusFromError(err, resp429) should prefer resp: got %d", got)
	}
}

func assertAnError(s string) error {
	return &errWithMessage{s}
}

type errWithMessage struct{ msg string }

func (e *errWithMessage) Error() string { return e.msg }

func TestNewConfluentCloudClient_Validation(t *testing.T) {
	_, err := NewConfluentCloudClient(nil)
	if err == nil {
		t.Error("NewConfluentCloudClient(nil) expected error")
	}
	_, err = NewConfluentCloudClient(&types.ConfluentCredentials{})
	if err == nil {
		t.Error("NewConfluentCloudClient(empty creds) expected error")
	}
	creds := &types.ConfluentCredentials{ApiKey: "key", ApiSecret: "secret"}
	client, err := NewConfluentCloudClient(creds)
	if err != nil {
		t.Fatalf("NewConfluentCloudClient(valid) err = %v", err)
	}
	if client == nil || client.creds != creds {
		t.Error("client should hold credentials")
	}
}

func TestWithRetry_RetriesOn429ThenSucceeds(t *testing.T) {
	creds := &types.ConfluentCredentials{ApiKey: "key", ApiSecret: "secret"}
	c, err := NewConfluentCloudClient(creds)
	if err != nil {
		t.Fatalf("NewConfluentCloudClient: %v", err)
	}
	ctx := context.Background()
	attempts := 0
	errRet := c.withRetry(ctx, func() (int, error) {
		attempts++
		if attempts < 3 {
			return 429, errors.New("rate limited")
		}
		return 0, nil
	})
	if errRet != nil {
		t.Errorf("withRetry should succeed after retries: %v", errRet)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestWithRetry_ReturnsImmediatelyOnNonRetryable(t *testing.T) {
	creds := &types.ConfluentCredentials{ApiKey: "key", ApiSecret: "secret"}
	c, err := NewConfluentCloudClient(creds)
	if err != nil {
		t.Fatalf("NewConfluentCloudClient: %v", err)
	}
	ctx := context.Background()
	attempts := 0
	wantErr := errors.New("not found")
	errRet := c.withRetry(ctx, func() (int, error) {
		attempts++
		return 404, wantErr
	})
	if errRet != wantErr {
		t.Errorf("withRetry should return 404 error immediately: %v", errRet)
	}
	if attempts != 1 {
		t.Errorf("expected 1 attempt (no retry on 404), got %d", attempts)
	}
}
