package discover

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/confluentinc/kcp/internal/client"
	"github.com/confluentinc/kcp/internal/types"
)

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
