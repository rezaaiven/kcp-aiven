// Package client provides API clients for Confluent Cloud (org/v2, cmk/v2) and other backends.
package client

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	cmkv2 "github.com/confluentinc/ccloud-sdk-go-v2/cmk/v2"
	orgv2 "github.com/confluentinc/ccloud-sdk-go-v2/org/v2"
	"github.com/confluentinc/kcp/internal/types"
)

const (
	defaultMaxRetries     = 5
	defaultInitialBackoff = 2 * time.Second
	maxBackoff            = 30 * time.Second
)

// ConfluentCloudClient calls Confluent Cloud REST APIs (org/v2, cmk/v2) via the official
// ccloud-sdk-go-v2.
//
// Auth scope: use a Cloud API key (created in Confluent Cloud Console under API keys,
// or via the Cloud API) that has permission to list environments and clusters. Do not
// use a Kafka cluster–scoped API key: those are for Kafka REST and broker access only.
// Same API key + secret are used for both org/v2 (environments) and cmk/v2 (clusters).
//
// See: https://docs.confluent.io/cloud/current/api.html
type ConfluentCloudClient struct {
	creds     *types.ConfluentCredentials
	orgClient *orgv2.APIClient
	cmkClient *cmkv2.APIClient
}

// NewConfluentCloudClient builds a client for Confluent Cloud APIs using ccloud-sdk-go-v2.
// creds must be non-nil and valid.
func NewConfluentCloudClient(creds *types.ConfluentCredentials) (*ConfluentCloudClient, error) {
	if creds == nil {
		return nil, fmt.Errorf("confluent credentials are required")
	}
	if err := creds.Validate(); err != nil {
		return nil, err
	}
	orgCfg := orgv2.NewConfiguration()
	orgClient := orgv2.NewAPIClient(orgCfg)

	cmkCfg := cmkv2.NewConfiguration()
	cmkClient := cmkv2.NewAPIClient(cmkCfg)

	return &ConfluentCloudClient{
		creds:     creds,
		orgClient: orgClient,
		cmkClient: cmkClient,
	}, nil
}

// Environment is a Confluent Cloud environment (org/v2).
type Environment struct {
	ID          string
	DisplayName string
}

// Cluster is a Confluent Cloud Kafka cluster (cmk/v2) with bootstrap and REST endpoints.
type Cluster struct {
	ID                    string
	DisplayName           string
	KafkaBootstrapEndpoint string
	HTTPEndpoint         string
	APIEndpoint          string
}

// isRetryableStatusCode returns true for 429 (rate limit) and 5xx (server errors).
func isRetryableStatusCode(code int) bool {
	return code == 429 || (code >= 500 && code < 600)
}

// withRetry runs fn and on retryable errors (429, 5xx) retries with exponential backoff.
// fn returns (HTTP status code, error). If resp is nil when err != nil, pass 0.
func (c *ConfluentCloudClient) withRetry(ctx context.Context, fn func() (statusCode int, err error)) error {
	var lastErr error
	backoff := defaultInitialBackoff
	for attempt := 0; attempt <= defaultMaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
		code, err := fn()
		if err == nil {
			return nil
		}
		lastErr = err
		if !isRetryableStatusCode(code) {
			return err
		}
	}
	return lastErr
}

// pageTokenFromNext extracts the page_token query parameter from a Confluent API "next" URL.
func pageTokenFromNext(nextURL string) string {
	if nextURL == "" {
		return ""
	}
	u, err := url.Parse(nextURL)
	if err != nil {
		return ""
	}
	return u.Query().Get("page_token")
}

// statusFromError returns the HTTP status code from resp when present, or tries to infer from err.
func statusFromError(err error, resp *http.Response) int {
	if resp != nil {
		return resp.StatusCode
	}
	if err == nil {
		return 0
	}
	// Some SDKs put status in error string, e.g. "429 Too Many Requests"
	if strings.Contains(err.Error(), "429") {
		return 429
	}
	return 0
}

// ListEnvironments returns all environments, following pagination. If creds.EnvironmentID
// is set, returns only that environment when it exists.
func (c *ConfluentCloudClient) ListEnvironments(ctx context.Context) ([]Environment, error) {
	ctx = context.WithValue(ctx, orgv2.ContextBasicAuth, orgv2.BasicAuth{
		UserName: c.creds.ApiKey,
		Password: c.creds.ApiSecret,
	})
	if c.creds.EnvironmentID != "" {
		var env orgv2.OrgV2Environment
		var resp *http.Response
		var execErr error
		retryErr := c.withRetry(ctx, func() (int, error) {
			env, resp, execErr = c.orgClient.EnvironmentsOrgV2Api.GetOrgV2Environment(ctx, c.creds.EnvironmentID).Execute()
			return statusFromError(execErr, resp), execErr
		})
		if retryErr != nil {
			if resp != nil && resp.StatusCode == 404 {
				return nil, fmt.Errorf("environment %s not found", c.creds.EnvironmentID)
			}
			return nil, fmt.Errorf("get environment: %w", retryErr)
		}
		return []Environment{{
			ID:          env.GetId(),
			DisplayName: env.GetDisplayName(),
		}}, nil
	}

	var all []Environment
	var pageToken string
	for {
		req := c.orgClient.EnvironmentsOrgV2Api.ListOrgV2Environments(ctx)
		if pageToken != "" {
			req = req.PageToken(pageToken)
		}
		var list orgv2.OrgV2EnvironmentList
		var resp *http.Response
		var execErr error
		retryErr := c.withRetry(ctx, func() (int, error) {
			list, resp, execErr = req.Execute()
			return statusFromError(execErr, resp), execErr
		})
		if retryErr != nil {
			return nil, fmt.Errorf("list environments: %w", retryErr)
		}
		for _, e := range list.GetData() {
			all = append(all, Environment{
				ID:          e.GetId(),
				DisplayName: e.GetDisplayName(),
			})
		}
		meta := list.GetMetadata()
		next := (&meta).GetNext()
		if next == "" {
			break
		}
		pageToken = pageTokenFromNext(next)
		if pageToken == "" {
			break
		}
		_ = resp
	}
	return all, nil
}

// ListClusters returns all Kafka clusters in the given environment, following pagination.
func (c *ConfluentCloudClient) ListClusters(ctx context.Context, environmentID string) ([]Cluster, error) {
	if environmentID == "" {
		return nil, fmt.Errorf("environment ID is required to list clusters")
	}
	ctx = context.WithValue(ctx, cmkv2.ContextBasicAuth, cmkv2.BasicAuth{
		UserName: c.creds.ApiKey,
		Password: c.creds.ApiSecret,
	})

	var all []Cluster
	var pageToken string
	for {
		req := c.cmkClient.ClustersCmkV2Api.ListCmkV2Clusters(ctx).Environment(environmentID)
		if pageToken != "" {
			req = req.PageToken(pageToken)
		}
		var list cmkv2.CmkV2ClusterList
		var resp *http.Response
		var execErr error
		retryErr := c.withRetry(ctx, func() (int, error) {
			list, resp, execErr = req.Execute()
			return statusFromError(execErr, resp), execErr
		})
		if retryErr != nil {
			return nil, fmt.Errorf("list clusters: %w", retryErr)
		}
		for _, item := range list.GetData() {
			spec := item.GetSpec()
			all = append(all, Cluster{
				ID:                    item.GetId(),
				DisplayName:           spec.GetDisplayName(),
				KafkaBootstrapEndpoint: spec.GetKafkaBootstrapEndpoint(),
				HTTPEndpoint:          spec.GetHttpEndpoint(),
				APIEndpoint:           spec.GetApiEndpoint(),
			})
		}
		meta := list.GetMetadata()
		next := (&meta).GetNext()
		if next == "" {
			break
		}
		pageToken = pageTokenFromNext(next)
		if pageToken == "" {
			break
		}
		_ = resp
	}
	return all, nil
}
