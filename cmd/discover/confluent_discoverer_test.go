package discover

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/confluentinc/kcp/internal/client"
	"github.com/confluentinc/kcp/internal/types"
)

// Ensure the real client satisfies the interface used by RunDiscoverConfluent.
var _ confluentLister = (*client.ConfluentCloudClient)(nil)

func TestRunDiscoverConfluent_NilClient(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	err := RunDiscoverConfluent(context.Background(), nil, path)
	if err == nil {
		t.Error("RunDiscoverConfluent(nil client) expected error")
	}
}

func TestRunDiscoverConfluent_EmptyStatePath(t *testing.T) {
	creds := &types.ConfluentCredentials{ApiKey: "k", ApiSecret: "s"}
	cc, err := client.NewConfluentCloudClient(creds)
	if err != nil {
		t.Fatalf("NewConfluentCloudClient: %v", err)
	}
	err = RunDiscoverConfluent(context.Background(), cc, "")
	if err == nil {
		t.Error("RunDiscoverConfluent(empty path) expected error")
	}
}

// fakeConfluentLister returns predefined environments and clusters for testing without the real API.
type fakeConfluentLister struct {
	envs     []client.Environment
	clusters map[string][]client.Cluster // key = environment ID
	listEnvsErr error
	listClustersErr error
}

func (f *fakeConfluentLister) ListEnvironments(ctx context.Context) ([]client.Environment, error) {
	if f.listEnvsErr != nil {
		return nil, f.listEnvsErr
	}
	return f.envs, nil
}

func (f *fakeConfluentLister) ListClusters(ctx context.Context, environmentID string) ([]client.Cluster, error) {
	if f.listClustersErr != nil {
		return nil, f.listClustersErr
	}
	if f.clusters != nil {
		if c, ok := f.clusters[environmentID]; ok {
			return c, nil
		}
	}
	return nil, nil
}

func TestRunDiscoverConfluent_Success_WithMockedData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kcp-state-confluent.json")

	lister := &fakeConfluentLister{
		envs: []client.Environment{
			{ID: "env-prod", DisplayName: "Production"},
		},
		clusters: map[string][]client.Cluster{
			"env-prod": {
				{
					ID:                    "lkc-abc123",
					DisplayName:           "my-kafka",
					KafkaBootstrapEndpoint: "pkc-xxx.us-east-1.aws.confluent.cloud:9092",
					HTTPEndpoint:           "https://pkc-xxx.us-east-1.aws.confluent.cloud",
					APIEndpoint:            "https://pkc-xxx.us-east-1.aws.confluent.cloud",
				},
				{
					ID:                    "lkc-def456",
					DisplayName:           "other-kafka",
					KafkaBootstrapEndpoint: "pkc-yyy.us-west-2.aws.confluent.cloud:9092",
					HTTPEndpoint:           "https://pkc-yyy.us-west-2.aws.confluent.cloud",
					APIEndpoint:            "https://pkc-yyy.us-west-2.aws.confluent.cloud",
				},
			},
		},
	}

	err := RunDiscoverConfluent(context.Background(), lister, path)
	if err != nil {
		t.Fatalf("RunDiscoverConfluent: %v", err)
	}

	loaded, err := types.LoadConfluentMigrationState(path)
	if err != nil {
		t.Fatalf("LoadConfluentMigrationState: %v", err)
	}
	if loaded.SourceType != types.SourceTypeConfluentCloud {
		t.Errorf("source_type = %q, want %q", loaded.SourceType, types.SourceTypeConfluentCloud)
	}
	if len(loaded.Environments) != 1 {
		t.Errorf("len(Environments) = %d, want 1", len(loaded.Environments))
	}
	if len(loaded.Clusters) != 2 {
		t.Errorf("len(Clusters) = %d, want 2", len(loaded.Clusters))
	}
	if loaded.Environments[0].ID != "env-prod" || loaded.Environments[0].DisplayName != "Production" {
		t.Errorf("Environments[0] = %+v", loaded.Environments[0])
	}
	var foundABC bool
	for _, c := range loaded.Clusters {
		if c.ClusterID == "lkc-abc123" {
			foundABC = true
			if c.EnvironmentID != "env-prod" || c.DisplayName != "my-kafka" {
				t.Errorf("cluster lkc-abc123 = %+v", c)
			}
			if c.KafkaBootstrapEndpoint != "pkc-xxx.us-east-1.aws.confluent.cloud:9092" {
				t.Errorf("KafkaBootstrapEndpoint = %q", c.KafkaBootstrapEndpoint)
			}
			break
		}
	}
	if !foundABC {
		t.Error("cluster lkc-abc123 not found in state")
	}
}

func TestRunDiscoverConfluent_ListEnvironmentsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	wantErr := errors.New("api unavailable")
	lister := &fakeConfluentLister{listEnvsErr: wantErr}
	err := RunDiscoverConfluent(context.Background(), lister, path)
	if err == nil {
		t.Fatal("RunDiscoverConfluent expected error")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("error should wrap wantErr: %v", err)
	}
	if err.Error() != "list environments: api unavailable" {
		t.Errorf("error message = %q", err.Error())
	}
}

func TestRunDiscoverConfluent_ListClustersError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	wantErr := errors.New("forbidden")
	lister := &fakeConfluentLister{
		envs:            []client.Environment{{ID: "env-1", DisplayName: "Env1"}},
		listClustersErr: wantErr,
	}
	err := RunDiscoverConfluent(context.Background(), lister, path)
	if err == nil {
		t.Fatal("RunDiscoverConfluent expected error")
	}
	if err.Error() != "list clusters for environment env-1: forbidden" {
		t.Errorf("err = %v", err)
	}
}

func TestRunDiscoverConfluent_EmptyEnvironments_WritesValidState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	lister := &fakeConfluentLister{envs: nil}
	err := RunDiscoverConfluent(context.Background(), lister, path)
	if err != nil {
		t.Fatalf("RunDiscoverConfluent(empty envs): %v", err)
	}
	loaded, err := types.LoadConfluentMigrationState(path)
	if err != nil {
		t.Fatalf("LoadConfluentMigrationState: %v", err)
	}
	if len(loaded.Environments) != 0 || len(loaded.Clusters) != 0 {
		t.Errorf("expected 0 envs and 0 clusters, got %d envs, %d clusters", len(loaded.Environments), len(loaded.Clusters))
	}
}

func TestRunDiscoverConfluent_TwoEnvironments_WithClusters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	lister := &fakeConfluentLister{
		envs: []client.Environment{
			{ID: "env-a", DisplayName: "Env A"},
			{ID: "env-b", DisplayName: "Env B"},
		},
		clusters: map[string][]client.Cluster{
			"env-a": {{ID: "lkc-a1", DisplayName: "Cluster A1", KafkaBootstrapEndpoint: "pkc-a:9092", HTTPEndpoint: "https://pkc-a", APIEndpoint: "https://pkc-a"}},
			"env-b": {{ID: "lkc-b1", DisplayName: "Cluster B1", KafkaBootstrapEndpoint: "pkc-b:9092", HTTPEndpoint: "https://pkc-b", APIEndpoint: "https://pkc-b"}},
		},
	}
	err := RunDiscoverConfluent(context.Background(), lister, path)
	if err != nil {
		t.Fatalf("RunDiscoverConfluent: %v", err)
	}
	loaded, err := types.LoadConfluentMigrationState(path)
	if err != nil {
		t.Fatalf("LoadConfluentMigrationState: %v", err)
	}
	if len(loaded.Environments) != 2 || len(loaded.Clusters) != 2 {
		t.Errorf("expected 2 envs and 2 clusters, got %d envs, %d clusters", len(loaded.Environments), len(loaded.Clusters))
	}
	envIDs := make(map[string]bool)
	for _, e := range loaded.Environments {
		envIDs[e.ID] = true
	}
	if !envIDs["env-a"] || !envIDs["env-b"] {
		t.Errorf("expected both env-a and env-b in state, got %v", loaded.Environments)
	}
	for _, c := range loaded.Clusters {
		if c.EnvironmentID != "env-a" && c.EnvironmentID != "env-b" {
			t.Errorf("cluster %s has unexpected EnvironmentID %q", c.ClusterID, c.EnvironmentID)
		}
	}
}
