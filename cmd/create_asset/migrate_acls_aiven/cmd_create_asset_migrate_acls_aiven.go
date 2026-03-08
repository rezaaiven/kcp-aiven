package migrate_acls_aiven

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/confluentinc/kcp/internal/services/hcl"
	"github.com/confluentinc/kcp/internal/types"
	"github.com/confluentinc/kcp/internal/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	aaAclFile                string
	aaAivenProject           string
	aaAivenKafkaServiceName  string
	aaOutputDir              string
)

func NewMigrateAclsAivenCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate-acls-aiven",
		Short: "Generate Aiven Kafka ACL Terraform from an ACL file",
		Long:  "Generate Terraform (aiven_kafka_acl) for Aiven Kafka from a JSON file containing ACL definitions. Only topic-level ACLs are emitted. Use an ACL file exported from Confluent (or the same JSON shape as kcp migrate-acls kafka expects).",
		SilenceErrors: true,
		PreRunE:       preRunMigrateAclsAiven,
		RunE:          runMigrateAclsAiven,
	}

	flags := pflag.NewFlagSet("migrate-acls-aiven", pflag.ExitOnError)
	flags.SortFlags = false
	flags.StringVar(&aaAclFile, "acl-file", "", "Path to JSON file with ACLs: array of {Principal, ResourceType, ResourceName, Operation, PermissionType} or object with \"acls\" key (required)")
	flags.StringVar(&aaAivenProject, "aiven-project", "", "Aiven project name (required)")
	flags.StringVar(&aaAivenKafkaServiceName, "aiven-kafka-service-name", "", "Aiven Kafka service name (required)")
	flags.StringVar(&aaOutputDir, "output-dir", "migrate_acls_aiven", "Output directory for generated Terraform files")
	cmd.Flags().AddFlagSet(flags)

	_ = cmd.MarkFlagRequired("acl-file")
	_ = cmd.MarkFlagRequired("aiven-project")
	_ = cmd.MarkFlagRequired("aiven-kafka-service-name")

	cmd.SetUsageFunc(func(c *cobra.Command) error {
		fmt.Fprintf(c.OutOrStderr(), "%s\n\n", c.Long)
		fmt.Fprintf(c.OutOrStderr(), "Usage:\n  %s [flags]\n\n", c.CommandPath())
		fmt.Fprintf(c.OutOrStderr(), "Flags:\n%s\n", flags.FlagUsages())
		fmt.Fprintln(c.OutOrStderr(), "ACL file format: JSON array of objects with Principal, ResourceType, ResourceName, Operation, PermissionType. Only ResourceType TOPIC is converted.")
		return nil
	})

	return cmd
}

func preRunMigrateAclsAiven(cmd *cobra.Command, args []string) error {
	if err := utils.BindEnvToFlags(cmd); err != nil {
		return err
	}
	aaAivenProject = strings.TrimSpace(aaAivenProject)
	aaAivenKafkaServiceName = strings.TrimSpace(aaAivenKafkaServiceName)
	if aaAclFile == "" || aaAivenProject == "" || aaAivenKafkaServiceName == "" {
		return fmt.Errorf("acl-file, aiven-project, and aiven-kafka-service-name are required")
	}
	return nil
}

// aclFileRoot supports both [ {...}, ... ] and { "acls": [ ... ] }.
type aclFileRoot struct {
	ACLs []types.Acls `json:"acls"`
}

func loadACLsFromFile(path string) ([]types.Acls, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read ACL file: %w", err)
	}

	var raw json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse JSON: %w", err)
	}

	// Try array first
	var arr []types.Acls
	if err := json.Unmarshal(raw, &arr); err == nil {
		return arr, nil
	}

	var obj aclFileRoot
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("ACL file must be a JSON array or object with \"acls\" key: %w", err)
	}
	return obj.ACLs, nil
}

func runMigrateAclsAiven(cmd *cobra.Command, args []string) error {
	slog.Info("🏁 generating Aiven Kafka ACL Terraform from ACL file")

	acls, err := loadACLsFromFile(aaAclFile)
	if err != nil {
		return err
	}

	aclsByPrincipal := make(map[string][]types.Acls)
	for _, acl := range acls {
		p := acl.Principal
		if p == "" {
			p = "unknown"
		}
		aclsByPrincipal[p] = append(aclsByPrincipal[p], acl)
	}

	project, err := hcl.GenerateTerraformFilesFromACLs(aaAivenProject, aaAivenKafkaServiceName, aclsByPrincipal)
	if err != nil {
		return fmt.Errorf("generate Terraform: %w", err)
	}

	if err := os.MkdirAll(aaOutputDir, 0755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	files := map[string]string{
		"main.tf":            project.MainTf,
		"providers.tf":       project.ProvidersTf,
		"variables.tf":       project.VariablesTf,
		"outputs.tf":         project.OutputsTf,
		"inputs.auto.tfvars": project.InputsAutoTfvars,
		"README.md":          project.ReadmeMd,
	}
	for name, content := range files {
		if content == "" {
			continue
		}
		if err := os.WriteFile(filepath.Join(aaOutputDir, name), []byte(content), 0644); err != nil {
			return fmt.Errorf("write %s: %w", name, err)
		}
		slog.Info("✅ wrote " + name)
	}

	slog.Info("✅ Aiven Kafka ACL Terraform generated", "directory", aaOutputDir)
	slog.Info("💡 set AIVEN_TOKEN (or aiven_api_token) when running terraform apply")
	return nil
}
