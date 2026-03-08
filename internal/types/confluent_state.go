package types

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/kcp/internal/build_info"
)

// SourceTypeConfluentCloud is the discriminator for Confluent-sourced migration state.
const SourceTypeConfluentCloud = "confluent_cloud"

// DefaultConfluentStateFilename is the default file name for Confluent migration state.
const DefaultConfluentStateFilename = "kcp-state-confluent.json"

// ConfluentMigrationState is the state shape for Confluent Cloud discovery.
// It is separate from State (MSK); downstream Confluent→Aiven commands read this file only.
// See docs/AIVEN_MIGRATION_PLAN.md Phase 1.
type ConfluentMigrationState struct {
	SourceType   string                 `json:"source_type"`
	Environments []ConfluentEnvironment `json:"environments"`
	Clusters     []ConfluentClusterInfo `json:"clusters"`
	KcpBuildInfo KcpBuildInfo           `json:"kcp_build_info"`
	Timestamp    time.Time              `json:"timestamp"`
}

// ConfluentEnvironment represents a Confluent Cloud environment (org/v2).
type ConfluentEnvironment struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
}

// ConfluentClusterInfo represents a Confluent Cloud Kafka cluster (cmk/v2) with endpoints.
type ConfluentClusterInfo struct {
	EnvironmentID           string `json:"environment_id"`
	ClusterID               string `json:"cluster_id"`
	DisplayName             string `json:"display_name"`
	KafkaBootstrapEndpoint  string `json:"kafka_bootstrap_endpoint"`
	HTTPEndpoint            string `json:"http_endpoint"`
	APIEndpoint             string `json:"api_endpoint"`
}

// NewConfluentMigrationState returns a new Confluent migration state with current build info and timestamp.
// Environments and Clusters are initialized to empty slices so JSON has "environments": [] and "clusters": []
// (consistent with State.Regions) and consumers need not nil-check.
func NewConfluentMigrationState() *ConfluentMigrationState {
	return &ConfluentMigrationState{
		SourceType:   SourceTypeConfluentCloud,
		Environments: []ConfluentEnvironment{},
		Clusters:     []ConfluentClusterInfo{},
		KcpBuildInfo: KcpBuildInfo{
			Version: build_info.Version,
			Commit:  build_info.Commit,
			Date:    build_info.Date,
		},
		Timestamp: time.Now(),
	}
}

// WriteToFile marshals the state to JSON and writes it to filePath.
func (s *ConfluentMigrationState) WriteToFile(filePath string) error {
	if s == nil {
		return fmt.Errorf("confluent migration state is nil")
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal confluent state: %w", err)
	}
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("write confluent state file: %w", err)
	}
	return nil
}

// PersistStateFile writes the state to filePath. It is the same as WriteToFile and exists for parity with State.
func (s *ConfluentMigrationState) PersistStateFile(filePath string) error {
	return s.WriteToFile(filePath)
}

// LoadConfluentMigrationState reads and unmarshals a Confluent migration state file.
// It returns an error if source_type is missing or not "confluent_cloud".
func LoadConfluentMigrationState(filePath string) (*ConfluentMigrationState, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("read confluent state file: %w", err)
	}
	var state ConfluentMigrationState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("unmarshal confluent state: %w", err)
	}
	if state.SourceType != SourceTypeConfluentCloud {
		return nil, fmt.Errorf("invalid source_type %q, expected %q", state.SourceType, SourceTypeConfluentCloud)
	}
	// Normalize nil slices so callers can always range without nil checks (JSON may have "environments": null).
	if state.Environments == nil {
		state.Environments = []ConfluentEnvironment{}
	}
	if state.Clusters == nil {
		state.Clusters = []ConfluentClusterInfo{}
	}
	return &state, nil
}
