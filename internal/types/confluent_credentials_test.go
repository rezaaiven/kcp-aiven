package types

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfluentCredentials_Validate(t *testing.T) {
	tests := []struct {
		name    string
		c       *ConfluentCredentials
		wantErr bool
	}{
		{"nil", nil, true},
		{"empty", &ConfluentCredentials{}, true},
		{"missing secret", &ConfluentCredentials{ApiKey: "key"}, true},
		{"missing key", &ConfluentCredentials{ApiSecret: "secret"}, true},
		{"valid", &ConfluentCredentials{ApiKey: "key", ApiSecret: "secret"}, false},
		{"valid with env", &ConfluentCredentials{ApiKey: "k", ApiSecret: "s", EnvironmentID: "env-1"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.c.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLoadConfluentCredentialsFromFile(t *testing.T) {
	dir := t.TempDir()

	t.Run("missing file returns nil nil", func(t *testing.T) {
		c, err := LoadConfluentCredentialsFromFile(filepath.Join(dir, "nonexistent.yaml"))
		if err != nil {
			t.Errorf("expected nil error for missing file, got %v", err)
		}
		if c != nil {
			t.Errorf("expected nil credentials for missing file, got %+v", c)
		}
	})

	validPath := filepath.Join(dir, "creds.yaml")
	if err := os.WriteFile(validPath, []byte("api_key: mykey\napi_secret: mysecret\n"), 0600); err != nil {
		t.Fatal(err)
	}

	t.Run("valid file", func(t *testing.T) {
		c, err := LoadConfluentCredentialsFromFile(validPath)
		if err != nil {
			t.Fatalf("LoadConfluentCredentialsFromFile() err = %v", err)
		}
		if c == nil {
			t.Fatal("expected non-nil credentials")
		}
		if c.ApiKey != "mykey" || c.ApiSecret != "mysecret" {
			t.Errorf("got ApiKey=%q ApiSecret=%q", c.ApiKey, c.ApiSecret)
		}
	})

	invalidPath := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(invalidPath, []byte("api_key: only\n"), 0600); err != nil {
		t.Fatal(err)
	}

	t.Run("invalid file missing secret", func(t *testing.T) {
		_, err := LoadConfluentCredentialsFromFile(invalidPath)
		if err == nil {
			t.Error("expected error when api_secret is missing")
		}
	})
}

func TestConfluentCredentialsFromEnv(t *testing.T) {
	// Clear env to avoid leaking from test runner
	restore := setConfluentEnv("", "", "")
	defer restore()

	t.Run("empty env returns false", func(t *testing.T) {
		_, ok := ConfluentCredentialsFromEnv()
		if ok {
			t.Error("expected false when env not set")
		}
	})

	t.Run("only key set returns false", func(t *testing.T) {
		restore := setConfluentEnv("key1", "", "")
		defer restore()
		_, ok := ConfluentCredentialsFromEnv()
		if ok {
			t.Error("expected false when only CONFLUENT_API_KEY set")
		}
	})

	t.Run("both set returns credentials", func(t *testing.T) {
		restore := setConfluentEnv("key1", "secret1", "env-123")
		defer restore()
		c, ok := ConfluentCredentialsFromEnv()
		if !ok {
			t.Fatal("expected true when both set")
		}
		if c.ApiKey != "key1" || c.ApiSecret != "secret1" || c.EnvironmentID != "env-123" {
			t.Errorf("got %+v", c)
		}
	})
}

func TestGetConfluentCredentials(t *testing.T) {
	restore := setConfluentEnv("", "", "")
	defer restore()

	dir := t.TempDir()
	filePath := filepath.Join(dir, "confluent-credentials.yaml")

	t.Run("no file no env returns error", func(t *testing.T) {
		_, err := GetConfluentCredentials("")
		if err == nil {
			t.Error("expected error when no credentials")
		}
	})

	t.Run("env takes precedence over file", func(t *testing.T) {
		if err := os.WriteFile(filePath, []byte("api_key: filekey\napi_secret: filesecret\n"), 0600); err != nil {
			t.Fatal(err)
		}
		restore := setConfluentEnv("envkey", "envsecret", "")
		defer restore()
		c, err := GetConfluentCredentials(filePath)
		if err != nil {
			t.Fatalf("GetConfluentCredentials() err = %v", err)
		}
		if c.ApiKey != "envkey" || c.ApiSecret != "envsecret" {
			t.Errorf("env should override file: got ApiKey=%q ApiSecret=%q", c.ApiKey, c.ApiSecret)
		}
	})

	t.Run("file used when env not set", func(t *testing.T) {
		if err := os.WriteFile(filePath, []byte("api_key: filekey\napi_secret: filesecret\n"), 0600); err != nil {
			t.Fatal(err)
		}
		c, err := GetConfluentCredentials(filePath)
		if err != nil {
			t.Fatalf("GetConfluentCredentials() err = %v", err)
		}
		if c.ApiKey != "filekey" || c.ApiSecret != "filesecret" {
			t.Errorf("got ApiKey=%q ApiSecret=%q", c.ApiKey, c.ApiSecret)
		}
	})
}

func setConfluentEnv(key, secret, envID string) func() {
	oldKey := os.Getenv("CONFLUENT_API_KEY")
	oldSecret := os.Getenv("CONFLUENT_API_SECRET")
	oldEnvID := os.Getenv("CONFLUENT_ENVIRONMENT_ID")
	_ = os.Setenv("CONFLUENT_API_KEY", key)
	_ = os.Setenv("CONFLUENT_API_SECRET", secret)
	_ = os.Setenv("CONFLUENT_ENVIRONMENT_ID", envID)
	return func() {
		_ = os.Unsetenv("CONFLUENT_API_KEY")
		_ = os.Unsetenv("CONFLUENT_API_SECRET")
		_ = os.Unsetenv("CONFLUENT_ENVIRONMENT_ID")
		if oldKey != "" {
			_ = os.Setenv("CONFLUENT_API_KEY", oldKey)
		}
		if oldSecret != "" {
			_ = os.Setenv("CONFLUENT_API_SECRET", oldSecret)
		}
		if oldEnvID != "" {
			_ = os.Setenv("CONFLUENT_ENVIRONMENT_ID", oldEnvID)
		}
	}
}
