package types

import (
	"fmt"
	"os"
	"strings"

	"github.com/goccy/go-yaml"
)

// DefaultAivenCredentialsFileName is the default name for the Aiven credentials file.
const DefaultAivenCredentialsFileName = "aiven-credentials.yaml"

// AivenCredentials holds Aiven API credentials used for Terraform provider auth
// and (optionally) for KCP when calling Aiven API. Aiven uses a single API token
// for the control plane. Kafka SASL credentials are per-service and are obtained
// from Aiven after the Kafka service is created; they are not stored here.
type AivenCredentials struct {
	// ApiToken is the Aiven API authentication token (required for Terraform and Aiven API).
	ApiToken string `yaml:"api_token" json:"-"`
}

// GetAivenCredentials returns Aiven credentials from environment variables
// (AIVEN_TOKEN or AIVEN_API_TOKEN) or from the given YAML file.
// Environment variables take precedence over the file.
// Use this for create-asset target-infra-aiven and any command that needs Aiven API access.
func GetAivenCredentials(filePath string) (*AivenCredentials, error) {
	if c, ok := AivenCredentialsFromEnv(); ok {
		return c, nil
	}
	if filePath != "" {
		c, err := LoadAivenCredentialsFromFile(filePath)
		if err != nil {
			return nil, err
		}
		if c != nil {
			return c, nil
		}
	}
	return nil, fmt.Errorf("aiven credentials not found: set AIVEN_TOKEN or AIVEN_API_TOKEN, or provide a valid %s file (see docs/aiven-credentials.example.yaml)", DefaultAivenCredentialsFileName)
}

// AivenCredentialsFromEnv builds credentials from environment variables.
// Supports AIVEN_TOKEN (used by Aiven Terraform provider) and AIVEN_API_TOKEN.
// Returns (nil, false) if neither is set. Token is trimmed of surrounding whitespace.
func AivenCredentialsFromEnv() (*AivenCredentials, bool) {
	token := strings.TrimSpace(os.Getenv("AIVEN_TOKEN"))
	if token == "" {
		token = strings.TrimSpace(os.Getenv("AIVEN_API_TOKEN"))
	}
	if token == "" {
		return nil, false
	}
	return &AivenCredentials{ApiToken: token}, true
}

// LoadAivenCredentialsFromFile reads Aiven credentials from a YAML file.
// Expected format: api_token.
// Returns (nil, nil) if the file does not exist.
// Returns an error if the file exists but is invalid or missing required fields.
func LoadAivenCredentialsFromFile(path string) (*AivenCredentials, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read aiven credentials file: %w", err)
	}
	var c AivenCredentials
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse aiven credentials file: %w", err)
	}
	c.ApiToken = strings.TrimSpace(c.ApiToken)
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// Validate returns an error if required fields are missing.
func (c *AivenCredentials) Validate() error {
	if c == nil {
		return fmt.Errorf("aiven credentials are nil")
	}
	if c.ApiToken == "" {
		return fmt.Errorf("aiven api_token is required")
	}
	return nil
}

// WriteToFile writes the credentials to a YAML file.
// Prefer env vars in CI; use restricted file permissions (e.g. 0600) for local use.
func (c *AivenCredentials) WriteToFile(path string) error {
	if c == nil {
		return fmt.Errorf("aiven credentials are nil")
	}
	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("marshal aiven credentials: %w", err)
	}
	return os.WriteFile(path, data, 0600)
}
