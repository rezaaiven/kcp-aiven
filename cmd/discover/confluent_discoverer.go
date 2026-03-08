package discover

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/confluentinc/kcp/internal/client"
	"github.com/confluentinc/kcp/internal/types"
)

// confluentLister is the subset of Confluent Cloud API used by discover. It allows tests to use a mock.
type confluentLister interface {
	ListEnvironments(ctx context.Context) ([]client.Environment, error)
	ListClusters(ctx context.Context, environmentID string) ([]client.Cluster, error)
}

// RunDiscoverConfluent discovers Confluent Cloud environments and Kafka clusters using
// the given lister and writes state to stateFilePath. It does not modify the MSK State type.
func RunDiscoverConfluent(ctx context.Context, lister confluentLister, stateFilePath string) error {
	if lister == nil {
		return fmt.Errorf("confluent cloud client is nil")
	}
	if stateFilePath == "" {
		return fmt.Errorf("state file path is required")
	}
	slog.Info("starting Confluent Cloud discovery")

	state := types.NewConfluentMigrationState()

	envs, err := lister.ListEnvironments(ctx)
	if err != nil {
		return fmt.Errorf("list environments: %w", err)
	}
	if len(envs) == 0 {
		slog.Info("no Confluent Cloud environments found (check credentials and environment_id scope)")
	}

	for _, e := range envs {
		state.Environments = append(state.Environments, types.ConfluentEnvironment{
			ID:          e.ID,
			DisplayName: e.DisplayName,
		})

		clusters, err := lister.ListClusters(ctx, e.ID)
		if err != nil {
			return fmt.Errorf("list clusters for environment %s: %w", e.ID, err)
		}

		for _, c := range clusters {
			state.Clusters = append(state.Clusters, types.ConfluentClusterInfo{
				EnvironmentID:          e.ID,
				ClusterID:              c.ID,
				DisplayName:            c.DisplayName,
				KafkaBootstrapEndpoint: c.KafkaBootstrapEndpoint,
				HTTPEndpoint:           c.HTTPEndpoint,
				APIEndpoint:            c.APIEndpoint,
			})
		}
	}

	if err := state.PersistStateFile(stateFilePath); err != nil {
		return fmt.Errorf("write state file: %w", err)
	}

	slog.Info("Confluent Cloud discovery complete",
		"state_file", stateFilePath,
		"environments", len(state.Environments),
		"clusters", len(state.Clusters))

	return nil
}
