//go:build confluent_live

package client

import (
	"context"
	"testing"
	"time"

	"github.com/confluentinc/kcp/internal/types"
)

// TestConfluentCloudClient_LiveAPI calls the real Confluent Cloud API (ListEnvironments, ListClusters).
// Run with: go test -tags=confluent_live ./internal/client/...
// Credentials: CONFLUENT_API_KEY, CONFLUENT_API_SECRET (or confluent-credentials.yaml).
// If credentials are missing, the test is skipped.
func TestConfluentCloudClient_LiveAPI(t *testing.T) {
	creds, err := types.GetConfluentCredentials(types.DefaultConfluentCredentialsFileName)
	if err != nil {
		t.Skipf("Confluent credentials not set (CONFLUENT_API_KEY/SECRET or %s): %v", types.DefaultConfluentCredentialsFileName, err)
	}

	cc, err := NewConfluentCloudClient(creds)
	if err != nil {
		t.Fatalf("NewConfluentCloudClient: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	envs, err := cc.ListEnvironments(ctx)
	if err != nil {
		t.Fatalf("ListEnvironments: %v", err)
	}
	// Account may have zero or more environments.
	for i, e := range envs {
		if e.ID == "" {
			t.Errorf("Environments[%d].ID is empty", i)
		}
	}

	if len(envs) > 0 {
		firstEnvID := envs[0].ID
		clusters, err := cc.ListClusters(ctx, firstEnvID)
		if err != nil {
			t.Fatalf("ListClusters(%q): %v", firstEnvID, err)
		}
		for i, c := range clusters {
			if c.ID == "" {
				t.Errorf("Clusters[%d].ID is empty", i)
			}
			if c.KafkaBootstrapEndpoint == "" && c.HTTPEndpoint == "" {
				t.Errorf("Clusters[%d] has no endpoints", i)
			}
		}
	}
}
