package migrate_schemas_aiven

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/confluentinc/kcp/internal/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	msConfluentSRURL        string
	msAivenProject          string
	msAivenKafkaServiceName string
	msSubjects              string // comma-separated; empty = all
	msOutputDir             string
)

func NewMigrateSchemasAivenCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate-schemas-aiven",
		Short: "Create script and docs to migrate schemas from Confluent Schema Registry to Aiven (Karapace)",
		Long:  "Generate a shell script and README to export schemas from Confluent Cloud Schema Registry and import them into Aiven Kafka's Karapace Schema Registry. Credentials are passed via environment variables at run time (not stored in generated files).",
		SilenceErrors: true,
		PreRunE:       preRunMigrateSchemasAiven,
		RunE:          runMigrateSchemasAiven,
	}

	flags := pflag.NewFlagSet("migrate-schemas-aiven", pflag.ExitOnError)
	flags.SortFlags = false
	flags.StringVar(&msConfluentSRURL, "confluent-sr-url", "", "Confluent Schema Registry URL (e.g. https://psrc-xxxxx.us-central1.gcp.confluent.cloud) (required)")
	flags.StringVar(&msAivenProject, "aiven-project", "", "Aiven project name (required)")
	flags.StringVar(&msAivenKafkaServiceName, "aiven-kafka-service-name", "", "Aiven Kafka service name (Karapace is enabled on this service) (required)")
	flags.StringVar(&msSubjects, "subjects", "", "Optional comma-separated list of subjects to migrate; empty = all subjects")
	flags.StringVar(&msOutputDir, "output-dir", "migrate_schemas_aiven", "Output directory for script and README")
	cmd.Flags().AddFlagSet(flags)

	_ = cmd.MarkFlagRequired("confluent-sr-url")
	_ = cmd.MarkFlagRequired("aiven-project")
	_ = cmd.MarkFlagRequired("aiven-kafka-service-name")

	cmd.SetUsageFunc(func(c *cobra.Command) error {
		fmt.Fprintf(c.OutOrStderr(), "%s\n\n", c.Long)
		fmt.Fprintf(c.OutOrStderr(), "Usage:\n  %s [flags]\n\n", c.CommandPath())
		fmt.Fprintf(c.OutOrStderr(), "Flags:\n%s\n", flags.FlagUsages())
		fmt.Fprintln(c.OutOrStderr(), "At script run time set CONFLUENT_SR_URL, CONFLUENT_SR_BASIC_AUTH (or CONFLUENT_SR_USER/CONFLUENT_SR_PASSWORD), AIVEN_SR_URL, and AIVEN_SR_ACCESS_TOKEN (or AIVEN_SR_BASIC_AUTH). See README in output directory.")
		return nil
	})

	return cmd
}

func preRunMigrateSchemasAiven(cmd *cobra.Command, args []string) error {
	if err := utils.BindEnvToFlags(cmd); err != nil {
		return err
	}
	msConfluentSRURL = strings.TrimSpace(msConfluentSRURL)
	msAivenProject = strings.TrimSpace(msAivenProject)
	msAivenKafkaServiceName = strings.TrimSpace(msAivenKafkaServiceName)
	if msConfluentSRURL == "" || msAivenProject == "" || msAivenKafkaServiceName == "" {
		return fmt.Errorf("confluent-sr-url, aiven-project, and aiven-kafka-service-name are required")
	}
	return nil
}

func runMigrateSchemasAiven(cmd *cobra.Command, args []string) error {
	slog.Info("🏁 generating Confluent → Aiven schema migration script and README")

	if err := os.MkdirAll(msOutputDir, 0755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	scriptPath := filepath.Join(msOutputDir, "export_import_schemas.sh")
	if err := os.WriteFile(scriptPath, []byte(exportImportScript), 0755); err != nil {
		return fmt.Errorf("write script: %w", err)
	}
	slog.Info("✅ wrote export_import_schemas.sh")

	readme := generateReadme()
	readmePath := filepath.Join(msOutputDir, "README.md")
	if err := os.WriteFile(readmePath, []byte(readme), 0644); err != nil {
		return fmt.Errorf("write README: %w", err)
	}
	slog.Info("✅ wrote README.md")

	slog.Info("✅ Confluent → Aiven schema migration assets generated", "directory", msOutputDir)
	slog.Info("💡 set CONFLUENT_SR_* and AIVEN_SR_* env vars when running ./export_import_schemas.sh")
	return nil
}

func generateReadme() string {
	return "# Migrate schemas from Confluent Schema Registry to Aiven (Karapace)\n\n" +
		"This directory contains a script to export schemas from Confluent Cloud Schema Registry and import them into Aiven Kafka's Karapace Schema Registry.\n\n" +
		"## Prerequisites\n\n" +
		"- Confluent Schema Registry URL (source)\n" +
		"- Aiven Kafka service with Karapace Schema Registry enabled (target). The Schema Registry URL is shown in Aiven console for your Kafka service.\n\n" +
		"## Environment variables (set before running the script)\n\n" +
		"| Variable | Description |\n" +
		"|----------|-------------|\n" +
		"| CONFLUENT_SR_URL | Confluent Schema Registry URL (same as --confluent-sr-url) |\n" +
		"| CONFLUENT_SR_BASIC_AUTH | Base64-encoded `user:password` for Confluent SR, or use CONFLUENT_SR_USER + CONFLUENT_SR_PASSWORD |\n" +
		"| AIVEN_SR_URL | Aiven Schema Registry URL (from Aiven Kafka service → Karapace) |\n" +
		"| AIVEN_SR_ACCESS_TOKEN | Aiven API token or basic auth (if required by your Aiven project) |\n" +
		"| AIVEN_SR_BASIC_AUTH | Alternative: base64-encoded `user:password` for Aiven SR |\n" +
		"| SUBJECTS | Optional: comma-separated list of subjects to migrate; unset = all subjects |\n\n" +
		"## Usage\n\n" +
		"```bash\n" +
		"export CONFLUENT_SR_URL=\"https://...\"\n" +
		"export CONFLUENT_SR_BASIC_AUTH=\"$(echo -n 'user:pass' | base64)\"\n" +
		"export AIVEN_SR_URL=\"https://...\"\n" +
		"export AIVEN_SR_ACCESS_TOKEN=\"...\"\n" +
		"./export_import_schemas.sh\n" +
		"```\n\n" +
		"## Limitations\n\n" +
		"- Karapace is largely compatible with Confluent Schema Registry API; some edge cases may require manual adjustment.\n" +
		"- Schema references and compatibility settings are best-effort; verify after migration.\n"
}
