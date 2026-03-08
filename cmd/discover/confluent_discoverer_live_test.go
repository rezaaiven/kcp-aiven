//go:build confluent_live

package discover

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/confluentinc/kcp/internal/client"
	"github.com/confluentinc/kcp/internal/types"
)

// TestRunDiscoverConfluent_LiveAPI runs discovery against the real Confluent Cloud API.
// It is only built and run with: go test -tags=confluent_live ./cmd/discover/...
// Credentials: set CONFLUENT_API_KEY and CONFLUENT_API_SECRET (or use confluent-credentials.yaml).
// If credentials are missing, the test is skipped.
func TestRunDiscoverConfluent_LiveAPI(t *testing.T) {
	creds, err := types.GetConfluentCredentials(types.DefaultConfluentCredentialsFileName)
	if err != nil {
		t.Skipf("Confluent credentials not set (CONFLUENT_API_KEY/SECRET or %s): %v", types.DefaultConfluentCredentialsFileName, err)
	}

	cc, err := client.NewConfluentCloudClient(creds)
	if err != nil {
		t.Fatalf("NewConfluentCloudClient: %v", err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "kcp-state-confluent.json")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := RunDiscoverConfluent(ctx, cc, path); err != nil {
		t.Fatalf("RunDiscoverConfluent: %v", err)
	}

	loaded, err := types.LoadConfluentMigrationState(path)
	if err != nil {
		t.Fatalf("LoadConfluentMigrationState: %v", err)
	}
	if loaded.SourceType != types.SourceTypeConfluentCloud {
		t.Errorf("source_type = %q, want %q", loaded.SourceType, types.SourceTypeConfluentCloud)
	}
	if loaded.Timestamp.IsZero() {
		t.Error("state Timestamp should be set")
	}
	// Do not assert exact env/cluster counts; they depend on the account.
}
