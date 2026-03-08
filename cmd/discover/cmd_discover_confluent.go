package discover

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/confluentinc/kcp/internal/client"
	"github.com/confluentinc/kcp/internal/types"
	"github.com/confluentinc/kcp/internal/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	confluentStateFile       string
	confluentCredentialsFile string
)

func NewDiscoverConfluentCmd() *cobra.Command {
	discoverConfluentCmd := &cobra.Command{
		Use:           "discover-confluent",
		Short:         "Discover Confluent Cloud environments and Kafka clusters",
		Long:          "Discovers Confluent Cloud environments and Kafka clusters using the Confluent Cloud API and writes state to a Confluent-specific state file (e.g. for Confluent → Aiven migration). Does not use or modify the MSK state file.",
		SilenceErrors: true,
		Args:          cobra.NoArgs,
		PreRunE:       preRunDiscoverConfluent,
		RunE:          runDiscoverConfluent,
	}

	optionalFlags := pflag.NewFlagSet("optional", pflag.ExitOnError)
	optionalFlags.SortFlags = false
	optionalFlags.StringVar(&confluentStateFile, "state-file", types.DefaultConfluentStateFilename, "Path to the Confluent migration state file to write")
	optionalFlags.StringVar(&confluentCredentialsFile, "credentials-file", types.DefaultConfluentCredentialsFileName, "Path to the Confluent credentials YAML file (env vars take precedence)")
	discoverConfluentCmd.Flags().AddFlagSet(optionalFlags)

	discoverConfluentCmd.SetUsageFunc(func(c *cobra.Command) error {
		fmt.Printf("%s\n\n", c.Short)
		usage := optionalFlags.FlagUsages()
		if usage != "" {
			fmt.Printf("Optional Flags:\n%s\n", usage)
		}
		fmt.Println("Credentials can be set via CONFLUENT_API_KEY, CONFLUENT_API_SECRET, and optionally CONFLUENT_ENVIRONMENT_ID.")
		fmt.Println("Flags can be provided via environment variables (e.g. STATE_FILE, CREDENTIALS_FILE).")
		return nil
	})

	return discoverConfluentCmd
}

func preRunDiscoverConfluent(cmd *cobra.Command, args []string) error {
	return utils.BindEnvToFlags(cmd)
}

func runDiscoverConfluent(cmd *cobra.Command, args []string) error {
	creds, err := types.GetConfluentCredentials(confluentCredentialsFile)
	if err != nil {
		return fmt.Errorf("confluent credentials: %w", err)
	}

	cc, err := client.NewConfluentCloudClient(creds)
	if err != nil {
		return fmt.Errorf("confluent cloud client: %w", err)
	}

	statePath := confluentStateFile
	if statePath == "" {
		statePath = types.DefaultConfluentStateFilename
	}

	ctx := context.Background()
	if err := RunDiscoverConfluent(ctx, cc, statePath); err != nil {
		slog.Error("discover-confluent failed", "error", err)
		return err
	}

	return nil
}
