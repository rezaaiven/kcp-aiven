package migrate_topics_aiven

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/confluentinc/kcp/internal/services/hcl"
	"github.com/confluentinc/kcp/internal/types"
	"github.com/confluentinc/kcp/internal/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	mmStateFile               string
	mmClusterId               string
	mmAivenProject            string
	mmAivenKafkaServiceName   string
	mmMirrormakerServiceName  string
	mmMirrormakerCloudName    string
	mmMirrormakerPlan         string
	mmTopicPattern            string
	mmOutputDir                string
	mmPreventDestroyStr        string
)

func NewMigrateTopicsAivenCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate-topics-aiven",
		Short: "Create Terraform for Confluent → Aiven replication (MirrorMaker 2)",
		Long:  "Generate Terraform for Aiven MirrorMaker 2 that replicates topics from Confluent Cloud to your Aiven Kafka service. Reads Confluent state from discover-confluent. Confluent API key/secret via variables at terraform apply (not stored in generated files).",
		SilenceErrors: true,
		PreRunE:       preRunMigrateTopicsAiven,
		RunE:          runMigrateTopicsAiven,
	}

	flags := pflag.NewFlagSet("migrate-topics-aiven", pflag.ExitOnError)
	flags.SortFlags = false
	flags.StringVar(&mmStateFile, "state-file", "", "Path to Confluent state file (kcp-state-confluent.json from discover-confluent) (required)")
	flags.StringVar(&mmClusterId, "cluster-id", "", "Confluent cluster ID to use as source (e.g. lkc-xxxxx) (required)")
	flags.StringVar(&mmAivenProject, "aiven-project", "", "Aiven project name (required)")
	flags.StringVar(&mmAivenKafkaServiceName, "aiven-kafka-service-name", "", "Name of the Aiven Kafka service (target from target-infra-aiven) (required)")
	flags.StringVar(&mmMirrormakerServiceName, "mirrormaker-service-name", "", "Name for the MirrorMaker 2 service (required)")
	flags.StringVar(&mmMirrormakerCloudName, "mirrormaker-cloud-name", "", "Cloud/region for MirrorMaker 2 (e.g. aws-eu-west-1). Defaults to same as Aiven Kafka if not set; pass explicitly if Kafka was created elsewhere.")
	flags.StringVar(&mmMirrormakerPlan, "mirrormaker-plan", "business-4", "MirrorMaker 2 plan")
	flags.StringVar(&mmTopicPattern, "topic-pattern", ".*", "Topic pattern to replicate (regex; default: .* for all topics)")
	flags.StringVar(&mmOutputDir, "output-dir", "migrate_topics_aiven", "Output directory for generated Terraform files")
	flags.StringVar(&mmPreventDestroyStr, "prevent-destroy", "true", "Set lifecycle { prevent_destroy = true } (true or false)")
	cmd.Flags().AddFlagSet(flags)

	_ = cmd.MarkFlagRequired("state-file")
	_ = cmd.MarkFlagRequired("cluster-id")
	_ = cmd.MarkFlagRequired("aiven-project")
	_ = cmd.MarkFlagRequired("aiven-kafka-service-name")
	_ = cmd.MarkFlagRequired("mirrormaker-service-name")

	cmd.SetUsageFunc(func(c *cobra.Command) error {
		fmt.Fprintf(c.OutOrStderr(), "%s\n\n", c.Long)
		fmt.Fprintf(c.OutOrStderr(), "Usage:\n  %s [flags]\n\n", c.CommandPath())
		fmt.Fprintf(c.OutOrStderr(), "Flags:\n%s\n", flags.FlagUsages())
		fmt.Fprintln(c.OutOrStderr(), "At terraform apply, set confluent_source_api_key and confluent_source_api_secret (or use CONFLUENT_API_KEY/CONFLUENT_API_SECRET if you map them). Do not commit secrets to inputs.auto.tfvars.")
		return nil
	})

	return cmd
}

func preRunMigrateTopicsAiven(cmd *cobra.Command, args []string) error {
	if err := utils.BindEnvToFlags(cmd); err != nil {
		return err
	}

	if _, err := strconv.ParseBool(mmPreventDestroyStr); err != nil {
		return fmt.Errorf("invalid value for --prevent-destroy: must be 'true' or 'false', got '%s'", mmPreventDestroyStr)
	}

	mmMirrormakerCloudName = strings.TrimSpace(mmMirrormakerCloudName)
	mmMirrormakerServiceName = strings.TrimSpace(mmMirrormakerServiceName)
	mmAivenKafkaServiceName = strings.TrimSpace(mmAivenKafkaServiceName)
	if mmMirrormakerServiceName == "" {
		return fmt.Errorf("--mirrormaker-service-name must be non-empty")
	}
	if mmAivenKafkaServiceName == "" {
		return fmt.Errorf("--aiven-kafka-service-name must be non-empty")
	}

	return nil
}

func runMigrateTopicsAiven(cmd *cobra.Command, args []string) error {
	slog.Info("🏁 generating Confluent → Aiven replication (MirrorMaker 2)")

	state, err := types.LoadConfluentMigrationState(mmStateFile)
	if err != nil {
		return fmt.Errorf("load Confluent state: %w", err)
	}

	var cluster *types.ConfluentClusterInfo
	for i := range state.Clusters {
		if state.Clusters[i].ClusterID == mmClusterId {
			cluster = &state.Clusters[i]
			break
		}
	}
	if cluster == nil {
		return fmt.Errorf("cluster-id %q not found in state file (have %d clusters)", mmClusterId, len(state.Clusters))
	}

	bootstrapServers := strings.TrimSpace(cluster.KafkaBootstrapEndpoint)
	if bootstrapServers == "" {
		return fmt.Errorf("cluster %q has empty kafka_bootstrap_endpoint in state", mmClusterId)
	}

	cloudName := mmMirrormakerCloudName
	if cloudName == "" {
		cloudName = "aws-eu-west-1" // sensible default; user can override
	}

	preventDestroy, _ := strconv.ParseBool(mmPreventDestroyStr)

	request := types.AivenMigrateTopicsRequest{
		ProjectName:              strings.TrimSpace(mmAivenProject),
		AivenKafkaServiceName:    mmAivenKafkaServiceName,
		MirrorMakerServiceName:   mmMirrormakerServiceName,
		MirrorMakerCloudName:     cloudName,
		MirrorMakerPlan:          strings.TrimSpace(mmMirrormakerPlan),
		ConfluentBootstrapServers: bootstrapServers,
		TopicPattern:             strings.TrimSpace(mmTopicPattern),
		PreventDestroy:           preventDestroy,
	}

	slog.Info("📋 generating Terraform configuration")
	svc := hcl.NewAivenMigrateTopicsHCLService()
	project := svc.GenerateTerraformFiles(request)

	slog.Info("📁 creating output directory", "directory", mmOutputDir)
	if err := os.MkdirAll(mmOutputDir, 0755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	generator := NewMigrateTopicsAivenGenerator(mmOutputDir)
	if err := generator.BuildTerraformProject(project); err != nil {
		return fmt.Errorf("write Terraform project: %w", err)
	}

	slog.Info("✅ Confluent → Aiven replication Terraform generated", "directory", mmOutputDir)
	slog.Info("💡 set AIVEN_TOKEN and confluent_source_api_key / confluent_source_api_secret when running terraform apply")
	return nil
}

// MigrateTopicsAivenGenerator writes a MigrationInfraTerraformProject to disk.
type MigrateTopicsAivenGenerator struct {
	OutputDir string
}

func NewMigrateTopicsAivenGenerator(outputDir string) *MigrateTopicsAivenGenerator {
	return &MigrateTopicsAivenGenerator{OutputDir: outputDir}
}

// orderedTerraformFiles is the fixed order for writing Terraform project files (deterministic output and logs).
var orderedTerraformFiles = []string{"main.tf", "providers.tf", "variables.tf", "outputs.tf", "inputs.auto.tfvars", "README.md"}

func (g *MigrateTopicsAivenGenerator) BuildTerraformProject(project types.MigrationInfraTerraformProject) error {
	files := map[string]string{
		"main.tf":            project.MainTf,
		"providers.tf":       project.ProvidersTf,
		"variables.tf":      project.VariablesTf,
		"outputs.tf":         project.OutputsTf,
		"inputs.auto.tfvars": project.InputsAutoTfvars,
		"README.md":          project.ReadmeMd,
	}
	for _, name := range orderedTerraformFiles {
		content := files[name]
		if content == "" {
			continue
		}
		if err := os.WriteFile(filepath.Join(g.OutputDir, name), []byte(content), 0644); err != nil {
			return fmt.Errorf("write %s: %w", name, err)
		}
		slog.Info("✅ wrote " + name)
	}
	if len(project.Modules) > 0 {
		return fmt.Errorf("migrate-topics-aiven does not support modules")
	}
	return nil
}
