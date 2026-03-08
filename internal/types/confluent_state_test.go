package types

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewConfluentMigrationState(t *testing.T) {
	s := NewConfluentMigrationState()
	if s == nil {
		t.Fatal("NewConfluentMigrationState() returned nil")
	}
	if s.SourceType != SourceTypeConfluentCloud {
		t.Errorf("SourceType = %q, want %q", s.SourceType, SourceTypeConfluentCloud)
	}
	if len(s.Environments) != 0 {
		t.Errorf("Environments should be empty initially, got len=%d", len(s.Environments))
	}
	if len(s.Clusters) != 0 {
		t.Errorf("Clusters should be empty initially, got len=%d", len(s.Clusters))
	}
	if s.KcpBuildInfo.Version == "" && s.KcpBuildInfo.Commit == "" && s.KcpBuildInfo.Date == "" {
		// At least one may be set by build_info
		t.Log("KcpBuildInfo may be empty in test environment")
	}
	if s.Timestamp.IsZero() {
		t.Error("Timestamp should be set")
	}
}

func TestConfluentMigrationState_WriteToFile_LoadConfluentMigrationState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kcp-state-confluent.json")

	s := NewConfluentMigrationState()
	s.Environments = []ConfluentEnvironment{
		{ID: "env-1", DisplayName: "Prod"},
	}
	s.Clusters = []ConfluentClusterInfo{
		{
			EnvironmentID:          "env-1",
			ClusterID:              "lkc-abc",
			DisplayName:            "my-kafka",
			KafkaBootstrapEndpoint: "pkc-xxx.us-east-1.aws.confluent.cloud:9092",
			HTTPEndpoint:           "https://pkc-xxx.us-east-1.aws.confluent.cloud",
			APIEndpoint:            "https://pkc-xxx.us-east-1.aws.confluent.cloud",
		},
	}

	if err := s.WriteToFile(path); err != nil {
		t.Fatalf("WriteToFile: %v", err)
	}

	loaded, err := LoadConfluentMigrationState(path)
	if err != nil {
		t.Fatalf("LoadConfluentMigrationState: %v", err)
	}
	if loaded.SourceType != SourceTypeConfluentCloud {
		t.Errorf("loaded SourceType = %q", loaded.SourceType)
	}
	if len(loaded.Environments) != 1 || loaded.Environments[0].ID != "env-1" {
		t.Errorf("loaded Environments = %v", loaded.Environments)
	}
	if len(loaded.Clusters) != 1 || loaded.Clusters[0].ClusterID != "lkc-abc" {
		t.Errorf("loaded Clusters = %v", loaded.Clusters)
	}
}

func TestConfluentMigrationState_WriteToFile_Nil(t *testing.T) {
	var s *ConfluentMigrationState
	err := s.WriteToFile(t.TempDir() + "/out.json")
	if err == nil {
		t.Error("WriteToFile(nil) expected error")
	}
}

func TestLoadConfluentMigrationState_InvalidSourceType(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	// Write a state file with wrong source_type
	err := os.WriteFile(path, []byte(`{"source_type":"aws_msk","environments":[],"clusters":[],"kcp_build_info":{},"timestamp":"2025-01-01T00:00:00Z"}`), 0644)
	if err != nil {
		t.Fatal(err)
	}
	_, err = LoadConfluentMigrationState(path)
	if err == nil {
		t.Error("LoadConfluentMigrationState(invalid source_type) expected error")
	}
}

func TestLoadConfluentMigrationState_NoFile(t *testing.T) {
	_, err := LoadConfluentMigrationState(filepath.Join(t.TempDir(), "nonexistent.json"))
	if err == nil {
		t.Error("LoadConfluentMigrationState(nonexistent) expected error")
	}
}

func TestConfluentMigrationState_RoundTripEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.json")
	s := NewConfluentMigrationState()
	if err := s.WriteToFile(path); err != nil {
		t.Fatalf("WriteToFile: %v", err)
	}
	loaded, err := LoadConfluentMigrationState(path)
	if err != nil {
		t.Fatalf("LoadConfluentMigrationState: %v", err)
	}
	if loaded.Environments == nil {
		t.Error("loaded.Environments should be non-nil (empty slice)")
	}
	if loaded.Clusters == nil {
		t.Error("loaded.Clusters should be non-nil (empty slice)")
	}
}

func TestLoadConfluentMigrationState_NullSlicesNormalized(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nulls.json")
	// JSON with null environments/clusters (e.g. from older writer or hand-edited)
	err := os.WriteFile(path, []byte(`{"source_type":"confluent_cloud","environments":null,"clusters":null,"kcp_build_info":{},"timestamp":"2025-01-01T00:00:00Z"}`), 0644)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadConfluentMigrationState(path)
	if err != nil {
		t.Fatalf("LoadConfluentMigrationState: %v", err)
	}
	if loaded.Environments == nil {
		t.Error("Load should normalize nil Environments to empty slice")
	}
	if loaded.Clusters == nil {
		t.Error("Load should normalize nil Clusters to empty slice")
	}
}

func TestLoadConfluentMigrationState_MalformedJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(path, []byte(`{invalid json`), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadConfluentMigrationState(path)
	if err == nil {
		t.Error("LoadConfluentMigrationState(malformed JSON) expected error")
	}
}

func TestConfluentMigrationState_PersistStateFile(t *testing.T) {
	s := NewConfluentMigrationState()
	path := filepath.Join(t.TempDir(), "out.json")
	if err := s.PersistStateFile(path); err != nil {
		t.Fatalf("PersistStateFile: %v", err)
	}
	if _, err := LoadConfluentMigrationState(path); err != nil {
		t.Fatalf("Load after PersistStateFile: %v", err)
	}
}
