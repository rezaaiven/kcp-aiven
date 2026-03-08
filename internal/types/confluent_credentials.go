package types

import (
	"fmt"
	"os"

	"github.com/goccy/go-yaml"
)

// DefaultConfluentCredentialsFileName is the default name for the Confluent credentials file.
const DefaultConfluentCredentialsFileName = "confluent-credentials.yaml"

// ConfluentCredentials holds Confluent Cloud API credentials used for discovery
// and (later) for Kafka REST and cluster linking. Confluent Cloud uses API key + secret
// for both the Cloud REST API and for Kafka SASL_SSL (API key as username, secret as password).
type ConfluentCredentials struct {
	// ApiKey is the Confluent Cloud API key (required for Cloud API and Kafka).
	ApiKey string `yaml:"api_key" json:"-"`
	// ApiSecret is the Confluent Cloud API secret (required).
	ApiSecret string `yaml:"api_secret" json:"-"`
	// EnvironmentID optionally restricts discovery to this Confluent environment.
	// If empty, the client may list all environments or use a default.
	EnvironmentID string `yaml:"environment_id,omitempty" json:"-"`
}

// GetConfluentCredentials returns Confluent credentials from environment variables
// (CONFLUENT_API_KEY, CONFLUENT_API_SECRET, CONFLUENT_ENVIRONMENT_ID) or from the
// given YAML file. Environment variables take precedence over the file.
// Use this for discover-confluent and any command that needs Confluent API access.
func GetConfluentCredentials(filePath string) (*ConfluentCredentials, error) {
	if c, ok := ConfluentCredentialsFromEnv(); ok {
		return c, nil
	}
	if filePath != "" {
		c, err := LoadConfluentCredentialsFromFile(filePath)
		if err != nil {
			return nil, err
		}
		if c != nil {
			return c, nil
		}
	}
	return nil, fmt.Errorf("confluent credentials not found: set CONFLUENT_API_KEY and CONFLUENT_API_SECRET, or provide a valid %s file (see docs/confluent-credentials.example.yaml)", DefaultConfluentCredentialsFileName)
}

// ConfluentCredentialsFromEnv builds credentials from environment variables.
// Returns (nil, false) if CONFLUENT_API_KEY or CONFLUENT_API_SECRET are missing.
func ConfluentCredentialsFromEnv() (*ConfluentCredentials, bool) {
	key := os.Getenv("CONFLUENT_API_KEY")
	secret := os.Getenv("CONFLUENT_API_SECRET")
	if key == "" || secret == "" {
		return nil, false
	}
	c := &ConfluentCredentials{
		ApiKey:        key,
		ApiSecret:     secret,
		EnvironmentID: os.Getenv("CONFLUENT_ENVIRONMENT_ID"),
	}
	return c, true
}

// LoadConfluentCredentialsFromFile reads Confluent credentials from a YAML file.
// Expected format: api_key, api_secret, and optional environment_id.
// Returns (nil, nil) if the file does not exist (caller can treat as "no credentials").
// Returns an error if the file exists but is invalid or missing required fields.
func LoadConfluentCredentialsFromFile(path string) (*ConfluentCredentials, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read confluent credentials file: %w", err)
	}
	var c ConfluentCredentials
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse confluent credentials file: %w", err)
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// Validate returns an error if required fields are missing.
func (c *ConfluentCredentials) Validate() error {
	if c == nil {
		return fmt.Errorf("confluent credentials are nil")
	}
	if c.ApiKey == "" {
		return fmt.Errorf("confluent api_key is required")
	}
	if c.ApiSecret == "" {
		return fmt.Errorf("confluent api_secret is required")
	}
	return nil
}

// WriteToFile writes the credentials to a YAML file (e.g. for saving after interactive setup).
// Only use on non-sensitive copies or with restricted permissions; prefer env vars in CI.
func (c *ConfluentCredentials) WriteToFile(path string) error {
	if c == nil {
		return fmt.Errorf("confluent credentials are nil")
	}
	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("marshal confluent credentials: %w", err)
	}
	return os.WriteFile(path, data, 0600)
}
