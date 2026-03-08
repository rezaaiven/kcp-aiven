package hcl

import (
	"github.com/confluentinc/kcp/internal/services/hcl/aiven"
	"github.com/confluentinc/kcp/internal/types"
	"github.com/confluentinc/kcp/internal/utils"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/zclconf/go-cty/cty"
)

const (
	aivenMigrateTopicsEndpointName    = "confluent_source"
	aivenMigrateTopicsMM2Name         = "mirrormaker"
	aivenMigrateTopicsIntegrationExt  = "confluent_to_mm2"
	aivenMigrateTopicsIntegrationKafka = "aiven_kafka_to_mm2"
	aivenMigrateTopicsReplicationFlow  = "confluent_to_aiven"
)

// AivenMigrateTopicsHCLService generates Terraform for Confluent → Aiven replication (Phase 2.2).
// Produces: external Kafka endpoint (Confluent), MirrorMaker 2 service, service integrations, replication flow.
type AivenMigrateTopicsHCLService struct{}

// NewAivenMigrateTopicsHCLService returns a new service.
func NewAivenMigrateTopicsHCLService() *AivenMigrateTopicsHCLService {
	return &AivenMigrateTopicsHCLService{}
}

// GetAivenMigrateTopicsVariableDefinitions returns variable definitions for migrate-topics-aiven.
func GetAivenMigrateTopicsVariableDefinitions() []types.TerraformVariable {
	return []types.TerraformVariable{
		{Name: aiven.VarAivenAPIToken, Description: "Aiven API token", Sensitive: true, Type: "string"},
		{Name: aiven.VarProjectName, Description: "Aiven project name", Sensitive: false, Type: "string"},
		{Name: aiven.VarAivenKafkaServiceName, Description: "Name of the Aiven Kafka service (target from 2.1)", Sensitive: false, Type: "string"},
		{Name: aiven.VarMirrormakerServiceName, Description: "Name of the MirrorMaker 2 service", Sensitive: false, Type: "string"},
		{Name: aiven.VarMirrormakerCloudName, Description: "Cloud/region for MirrorMaker 2", Sensitive: false, Type: "string"},
		{Name: aiven.VarMirrormakerPlan, Description: "MirrorMaker 2 plan", Sensitive: false, Type: "string"},
		{Name: aiven.VarConfluentBootstrapServers, Description: "Confluent Cloud bootstrap servers (comma-separated)", Sensitive: false, Type: "string"},
		{Name: aiven.VarExternalEndpointName, Description: "Name for the external Kafka endpoint", Sensitive: false, Type: "string"},
		{Name: aiven.VarConfluentSourceAPIKey, Description: "Confluent API key (SASL username)", Sensitive: true, Type: "string"},
		{Name: aiven.VarConfluentSourceAPISecret, Description: "Confluent API secret (SASL password)", Sensitive: true, Type: "string"},
		{Name: aiven.VarTopics, Description: "Topic patterns to replicate (e.g. [\".*\"] for all)", Sensitive: false, Type: "list(string)"},
	}
}

// GenerateTerraformFiles produces a Terraform project for Confluent → Aiven replication.
func (s *AivenMigrateTopicsHCLService) GenerateTerraformFiles(request types.AivenMigrateTopicsRequest) types.MigrationInfraTerraformProject {
	return types.MigrationInfraTerraformProject{
		MainTf:           s.generateRootMainTf(request),
		ProvidersTf:      s.generateRootProvidersTf(),
		VariablesTf:      s.generateVariablesTf(GetAivenMigrateTopicsVariableDefinitions()),
		OutputsTf:        s.generateOutputsTf(),
		InputsAutoTfvars: s.generateInputsAutoTfvars(request),
		ReadmeMd:         s.generateReadmeMd(),
		Modules:          nil,
	}
}

func (s *AivenMigrateTopicsHCLService) generateRootMainTf(request types.AivenMigrateTopicsRequest) string {
	f := hclwrite.NewEmptyFile()
	rootBody := f.Body()

	// 1. External Kafka endpoint (Confluent Cloud)
	rootBody.AppendBlock(aiven.GenerateExternalKafkaEndpoint(
		aivenMigrateTopicsEndpointName,
		aiven.VarProjectName,
		aiven.VarExternalEndpointName,
		aiven.VarConfluentBootstrapServers,
		aiven.VarConfluentSourceAPIKey,
		aiven.VarConfluentSourceAPISecret,
	))
	rootBody.AppendNewline()

	// 2. MirrorMaker 2 service
	rootBody.AppendBlock(aiven.GenerateKafkaMirrormaker(
		aivenMigrateTopicsMM2Name,
		aiven.VarProjectName,
		aiven.VarMirrormakerCloudName,
		aiven.VarMirrormakerPlan,
		aiven.VarMirrormakerServiceName,
		request.PreventDestroy,
	))
	rootBody.AppendNewline()

	// 3. Service integration: external endpoint → MM2 (source)
	rootBody.AppendBlock(aiven.GenerateServiceIntegrationEndpointToMirrormaker(
		aivenMigrateTopicsIntegrationExt,
		"aiven_service_integration_endpoint."+aivenMigrateTopicsEndpointName+".id",
		aiven.VarMirrormakerServiceName,
		aiven.DefaultSourceClusterAlias,
	))
	rootBody.AppendNewline()

	// 4. Service integration: Aiven Kafka → MM2 (target)
	rootBody.AppendBlock(aiven.GenerateServiceIntegrationKafkaToMirrormaker(
		aivenMigrateTopicsIntegrationKafka,
		aiven.VarAivenKafkaServiceName,
		aiven.VarMirrormakerServiceName,
		aiven.DefaultTargetClusterAlias,
	))
	rootBody.AppendNewline()

	// 5. Replication flow (depends on both integrations so aliases exist before flow is created)
	rootBody.AppendBlock(aiven.GenerateMirrormakerReplicationFlow(
		aivenMigrateTopicsReplicationFlow,
		aiven.VarProjectName,
		aiven.VarMirrormakerServiceName,
		aiven.DefaultSourceClusterAlias,
		aiven.DefaultTargetClusterAlias,
		aiven.VarTopics,
		[]string{
			"aiven_service_integration." + aivenMigrateTopicsIntegrationExt,
			"aiven_service_integration." + aivenMigrateTopicsIntegrationKafka,
		},
	))

	return string(f.Bytes())
}

func (s *AivenMigrateTopicsHCLService) generateRootProvidersTf() string {
	f := hclwrite.NewEmptyFile()
	rootBody := f.Body()

	terraformBlock := rootBody.AppendNewBlock("terraform", nil)
	terraformBody := terraformBlock.Body()
	requiredProvidersBlock := terraformBody.AppendNewBlock("required_providers", nil)
	requiredProvidersBody := requiredProvidersBlock.Body()
	requiredProvidersBody.SetAttributeRaw(aiven.GenerateRequiredProviderTokens())
	rootBody.AppendNewline()

	rootBody.AppendBlock(aiven.GenerateProviderBlock())
	rootBody.AppendNewline()

	return string(f.Bytes())
}

func (s *AivenMigrateTopicsHCLService) generateVariablesTf(tfVariables []types.TerraformVariable) string {
	f := hclwrite.NewEmptyFile()
	rootBody := f.Body()

	seen := make(map[string]bool)
	for _, v := range tfVariables {
		if seen[v.Name] {
			continue
		}
		seen[v.Name] = true

		variableBlock := rootBody.AppendNewBlock("variable", []string{v.Name})
		variableBody := variableBlock.Body()
		variableBody.SetAttributeRaw("type", utils.TokensForResourceReference(v.Type))
		if v.Description != "" {
			variableBody.SetAttributeValue("description", cty.StringVal(v.Description))
		}
		if v.Sensitive {
			variableBody.SetAttributeValue("sensitive", cty.BoolVal(true))
		}
		rootBody.AppendNewline()
	}

	return string(f.Bytes())
}

func (s *AivenMigrateTopicsHCLService) generateOutputsTf() string {
	f := hclwrite.NewEmptyFile()
	rootBody := f.Body()

	outputBlock := rootBody.AppendNewBlock("output", []string{"mirrormaker_service_name"})
	outputBody := outputBlock.Body()
	outputBody.SetAttributeRaw("value", utils.TokensForVarReference(aiven.VarMirrormakerServiceName))
	outputBody.SetAttributeValue("description", cty.StringVal("MirrorMaker 2 service name"))
	outputBody.SetAttributeValue("sensitive", cty.BoolVal(false))

	return string(f.Bytes())
}

func (s *AivenMigrateTopicsHCLService) generateInputsAutoTfvars(request types.AivenMigrateTopicsRequest) string {
	f := hclwrite.NewEmptyFile()
	rootBody := f.Body()

	rootBody.SetAttributeValue("project_name", cty.StringVal(request.ProjectName))
	rootBody.SetAttributeValue(aiven.VarAivenKafkaServiceName, cty.StringVal(request.AivenKafkaServiceName))
	rootBody.SetAttributeValue("mirrormaker_service_name", cty.StringVal(request.MirrorMakerServiceName))
	rootBody.SetAttributeValue("mirrormaker_cloud_name", cty.StringVal(request.MirrorMakerCloudName))
	rootBody.SetAttributeValue("mirrormaker_plan", cty.StringVal(request.MirrorMakerPlan))
	rootBody.SetAttributeValue("confluent_bootstrap_servers", cty.StringVal(request.ConfluentBootstrapServers))
	rootBody.SetAttributeValue("external_endpoint_name", cty.StringVal("confluent-source-endpoint"))

	topicPattern := request.TopicPattern
	if topicPattern == "" {
		topicPattern = ".*"
	}
	rootBody.SetAttributeValue("topics", cty.ListVal([]cty.Value{cty.StringVal(topicPattern)}))

	return string(f.Bytes())
}

func (s *AivenMigrateTopicsHCLService) generateReadmeMd() string {
	return "# Confluent Cloud → Aiven replication (MirrorMaker 2)\n\n" +
		"This Terraform creates an Aiven MirrorMaker 2 service and replicates topics from Confluent Cloud to your Aiven Kafka service.\n\n" +
		"## Prerequisites\n\n" +
		"- Aiven Kafka service (from `kcp create-asset target-infra-aiven`).\n" +
		"- Confluent Cloud cluster bootstrap and API key/secret (SASL_SSL).\n" +
		"- Aiven API token (AIVEN_TOKEN or aiven_api_token variable).\n\n" +
		"**Tip:** Set `mirrormaker_cloud_name` to the same cloud/region as your Aiven Kafka service when possible (e.g. same as used in target-infra-aiven).\n\n" +
		"## Authentication\n\n" +
		"- **Aiven:** Set AIVEN_TOKEN or use -var=\"aiven_api_token=...\".\n" +
		"- **Confluent (source):** Set confluent_source_api_key and confluent_source_api_secret via -var or TF_VAR_... (do not commit in tfvars).\n\n" +
		"## ACL and SASL requirements\n\n" +
		"- **Confluent Cloud** must use SASL_SSL; MirrorMaker 2 connects with API key as username and secret as password.\n" +
		"- **Source (Confluent):** MirrorMaker 2 needs READ on topics to replicate and on internal topics (e.g. consumer groups, heartbeats). Grant the Confluent API key appropriate ACLs.\n" +
		"- **Target (Aiven):** MirrorMaker 2 needs WRITE on target topics and internal topics. Use Aiven Kafka service credentials or ACLs as per Aiven docs.\n" +
		"- See [Aiven: Permissions and internal topics in MirrorMaker 2](https://aiven.io/docs/products/kafka/kafka-mirrormaker/concepts/permissions-internal-topics).\n\n" +
		"## Variables\n\n" +
		"| Variable | Description |\n" +
		"|----------|-------------|\n" +
		"| aiven_api_token | Aiven API token (sensitive) |\n" +
		"| project_name | Aiven project |\n" +
		"| " + aiven.VarAivenKafkaServiceName + " | Aiven Kafka service (target) |\n" +
		"| mirrormaker_service_name | MirrorMaker 2 service name |\n" +
		"| mirrormaker_cloud_name | Cloud/region for MM2 |\n" +
		"| mirrormaker_plan | MM2 plan |\n" +
		"| confluent_bootstrap_servers | Confluent bootstrap (comma-separated) |\n" +
		"| external_endpoint_name | Name for external Kafka endpoint |\n" +
		"| confluent_source_api_key | Confluent API key (sensitive) |\n" +
		"| confluent_source_api_secret | Confluent API secret (sensitive) |\n" +
		"| topics | List of topic patterns (e.g. [\".*\"]) |\n"
}
