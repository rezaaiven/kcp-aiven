package targetinfra

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"unicode"

	"github.com/confluentinc/kcp/internal/services/hcl"
	"github.com/confluentinc/kcp/internal/types"
	"github.com/confluentinc/kcp/internal/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	aivenOutputDir         string
	aivenProjectName       string
	aivenCloudName         string
	aivenPlan              string
	aivenServiceName       string
	aivenPreventDestroyStr string
)

func NewTargetInfraAivenCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "target-infra-aiven",
		Short: "Create Aiven for Kafka target infrastructure (Confluent → Aiven migration)",
		Long:  "Generate Terraform assets for an Aiven for Apache Kafka® service as a migration target. Use Confluent state from Phase 1 discover-confluent; credentials via AIVEN_TOKEN, AIVEN_API_TOKEN, or aiven-credentials.yaml.",
		SilenceErrors: true,
		PreRunE:       preRunCreateTargetInfraAiven,
		RunE:          runCreateTargetInfraAiven,
	}

	flags := pflag.NewFlagSet("target-infra-aiven", pflag.ExitOnError)
	flags.SortFlags = false
	flags.StringVar(&aivenOutputDir, "output-dir", "target_infra_aiven", "Output directory for generated Terraform files")
	flags.StringVar(&aivenProjectName, "project", "", "Aiven project name (required)")
	flags.StringVar(&aivenCloudName, "cloud-name", "", "Aiven cloud and region, e.g. google-europe-west1 or aws-eu-west-1 (required)")
	flags.StringVar(&aivenPlan, "plan", "business-4", "Aiven Kafka plan (e.g. business-4, startup-2)")
	flags.StringVar(&aivenServiceName, "service-name", "", "Unique name for the Kafka service (required)")
	flags.StringVar(&aivenPreventDestroyStr, "prevent-destroy", "true", "Set lifecycle { prevent_destroy = true } on resources (true or false)")
	cmd.Flags().AddFlagSet(flags)

	_ = cmd.MarkFlagRequired("project")
	_ = cmd.MarkFlagRequired("cloud-name")
	_ = cmd.MarkFlagRequired("service-name")

	cmd.SetUsageFunc(func(c *cobra.Command) error {
		fmt.Fprintf(c.OutOrStderr(), "%s\n\n", c.Long)
		fmt.Fprintf(c.OutOrStderr(), "Usage:\n  %s [flags]\n\n", c.CommandPath())
		fmt.Fprintf(c.OutOrStderr(), "Flags:\n%s\n", flags.FlagUsages())
		fmt.Fprintln(c.OutOrStderr(), "Credentials: set AIVEN_TOKEN or AIVEN_API_TOKEN, or use aiven-credentials.yaml (see docs/aiven-credentials.example.yaml).")
		return nil
	})

	return cmd
}

// validateAivenName checks that a value is non-empty, 1-64 chars, and only alphanumeric, hyphen, underscore.
// Used for project name and service name to avoid Terraform/Aiven errors.
func validateAivenName(name, flagName string) error {
	s := strings.TrimSpace(name)
	if s == "" {
		return fmt.Errorf("%s must be non-empty", flagName)
	}
	if len(s) > 64 {
		return fmt.Errorf("%s must be at most 64 characters, got %d", flagName, len(s))
	}
	for _, r := range s {
		if !unicode.IsLetter(r) && !unicode.IsNumber(r) && r != '-' && r != '_' {
			return fmt.Errorf("%s may only contain letters, digits, hyphen, and underscore", flagName)
		}
	}
	return nil
}

func preRunCreateTargetInfraAiven(cmd *cobra.Command, args []string) error {
	if err := utils.BindEnvToFlags(cmd); err != nil {
		return err
	}

	if _, err := strconv.ParseBool(aivenPreventDestroyStr); err != nil {
		return fmt.Errorf("invalid value for --prevent-destroy: must be 'true' or 'false', got '%s'", aivenPreventDestroyStr)
	}

	if err := validateAivenName(aivenProjectName, "--project"); err != nil {
		return err
	}
	if err := validateAivenName(aivenServiceName, "--service-name"); err != nil {
		return err
	}
	aivenCloudName = strings.TrimSpace(aivenCloudName)
	if aivenCloudName == "" {
		return fmt.Errorf("--cloud-name must be non-empty")
	}
	if len(aivenCloudName) > 64 {
		return fmt.Errorf("--cloud-name must be at most 64 characters, got %d", len(aivenCloudName))
	}
	aivenPlan = strings.TrimSpace(aivenPlan)
	if aivenPlan == "" {
		return fmt.Errorf("--plan must be non-empty")
	}

	return nil
}

func runCreateTargetInfraAiven(cmd *cobra.Command, args []string) error {
	slog.Info("🏁 generating Aiven target infrastructure")

	preventDestroy, _ := strconv.ParseBool(aivenPreventDestroyStr)

	request := types.AivenTargetRequest{
		ProjectName:    strings.TrimSpace(aivenProjectName),
		CloudName:      aivenCloudName,
		Plan:           aivenPlan,
		ServiceName:    strings.TrimSpace(aivenServiceName),
		PreventDestroy: preventDestroy,
	}

	slog.Info("📋 generating Terraform configuration")
	svc := hcl.NewAivenTargetInfraHCLService()
	project := svc.GenerateTerraformFiles(request)

	slog.Info("📁 creating output directory", "directory", aivenOutputDir)
	if err := os.MkdirAll(aivenOutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	generator := NewTargetInfraGenerator(aivenOutputDir)
	if err := generator.BuildTerraformProject(project); err != nil {
		return fmt.Errorf("failed to write Terraform project: %w", err)
	}

	slog.Info("✅ Aiven target infrastructure generated", "directory", aivenOutputDir)
	slog.Info("💡 set AIVEN_TOKEN (or aiven_api_token) when running terraform apply")
	return nil
}
