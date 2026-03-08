package types

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAivenCredentials_Validate(t *testing.T) {
	tests := []struct {
		name    string
		c       *AivenCredentials
		wantErr bool
	}{
		{"nil", nil, true},
		{"empty", &AivenCredentials{}, true},
		{"valid", &AivenCredentials{ApiToken: "token123"}, false},
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

func TestLoadAivenCredentialsFromFile(t *testing.T) {
	dir := t.TempDir()

	t.Run("missing file returns nil nil", func(t *testing.T) {
		c, err := LoadAivenCredentialsFromFile(filepath.Join(dir, "nonexistent.yaml"))
		if err != nil {
			t.Errorf("expected nil error for missing file, got %v", err)
		}
		if c != nil {
			t.Errorf("expected nil credentials for missing file, got %+v", c)
		}
	})

	validPath := filepath.Join(dir, "creds.yaml")
	if err := os.WriteFile(validPath, []byte("api_token: mytoken\n"), 0600); err != nil {
		t.Fatal(err)
	}

	t.Run("valid file", func(t *testing.T) {
		c, err := LoadAivenCredentialsFromFile(validPath)
		if err != nil {
			t.Fatalf("LoadAivenCredentialsFromFile() err = %v", err)
		}
		if c == nil {
			t.Fatal("expected non-nil credentials")
		}
		if c.ApiToken != "mytoken" {
			t.Errorf("got ApiToken=%q", c.ApiToken)
		}
	})

	t.Run("file token trimmed", func(t *testing.T) {
		trimPath := filepath.Join(dir, "trim.yaml")
		if err := os.WriteFile(trimPath, []byte("api_token: \"  file-token  \"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		c, err := LoadAivenCredentialsFromFile(trimPath)
		if err != nil {
			t.Fatalf("LoadAivenCredentialsFromFile() err = %v", err)
		}
		if c.ApiToken != "file-token" {
			t.Errorf("got ApiToken=%q (expected whitespace trimmed)", c.ApiToken)
		}
	})

	invalidPath := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(invalidPath, []byte("api_token: \"\n"), 0600); err != nil {
		t.Fatal(err)
	}

	t.Run("invalid file empty token", func(t *testing.T) {
		_, err := LoadAivenCredentialsFromFile(invalidPath)
		if err == nil {
			t.Error("expected error when api_token is empty")
		}
	})
}

func TestAivenCredentialsFromEnv(t *testing.T) {
	restore := func() {
		os.Unsetenv("AIVEN_TOKEN")
		os.Unsetenv("AIVEN_API_TOKEN")
	}
	defer restore()

	t.Run("no env returns false", func(t *testing.T) {
		restore()
		c, ok := AivenCredentialsFromEnv()
		if ok {
			t.Errorf("expected false when no env set, got credentials %+v", c)
		}
	})

	t.Run("AIVEN_TOKEN set", func(t *testing.T) {
		restore()
		os.Setenv("AIVEN_TOKEN", "env-token")
		defer restore()
		c, ok := AivenCredentialsFromEnv()
		if !ok {
			t.Fatal("expected true when AIVEN_TOKEN set")
		}
		if c.ApiToken != "env-token" {
			t.Errorf("got ApiToken=%q", c.ApiToken)
		}
	})

	t.Run("AIVEN_TOKEN trimmed", func(t *testing.T) {
		restore()
		os.Setenv("AIVEN_TOKEN", "  trimmed-token  ")
		defer restore()
		c, ok := AivenCredentialsFromEnv()
		if !ok {
			t.Fatal("expected true when AIVEN_TOKEN set")
		}
		if c.ApiToken != "trimmed-token" {
			t.Errorf("got ApiToken=%q (expected whitespace trimmed)", c.ApiToken)
		}
	})

	t.Run("AIVEN_API_TOKEN set", func(t *testing.T) {
		restore()
		os.Setenv("AIVEN_API_TOKEN", "api-token")
		defer restore()
		c, ok := AivenCredentialsFromEnv()
		if !ok {
			t.Fatal("expected true when AIVEN_API_TOKEN set")
		}
		if c.ApiToken != "api-token" {
			t.Errorf("got ApiToken=%q", c.ApiToken)
		}
	})
}

func TestGetAivenCredentials(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "aiven-credentials.yaml")
	if err := os.WriteFile(filePath, []byte("api_token: file-token\n"), 0600); err != nil {
		t.Fatal(err)
	}

	os.Unsetenv("AIVEN_TOKEN")
	os.Unsetenv("AIVEN_API_TOKEN")
	defer func() {
		os.Unsetenv("AIVEN_TOKEN")
		os.Unsetenv("AIVEN_API_TOKEN")
	}()

	t.Run("from file", func(t *testing.T) {
		c, err := GetAivenCredentials(filePath)
		if err != nil {
			t.Fatalf("GetAivenCredentials() err = %v", err)
		}
		if c.ApiToken != "file-token" {
			t.Errorf("got ApiToken=%q", c.ApiToken)
		}
	})

	t.Run("env overrides file", func(t *testing.T) {
		os.Setenv("AIVEN_TOKEN", "env-override")
		defer os.Unsetenv("AIVEN_TOKEN")
		c, err := GetAivenCredentials(filePath)
		if err != nil {
			t.Fatalf("GetAivenCredentials() err = %v", err)
		}
		if c.ApiToken != "env-override" {
			t.Errorf("got ApiToken=%q (expected env override)", c.ApiToken)
		}
	})

	t.Run("empty path and no env fails", func(t *testing.T) {
		os.Unsetenv("AIVEN_TOKEN")
		os.Unsetenv("AIVEN_API_TOKEN")
		_, err := GetAivenCredentials("")
		if err == nil {
			t.Error("expected error when no file path and no env")
		}
	})
}
