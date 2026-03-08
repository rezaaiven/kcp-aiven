package aiven

import (
	"github.com/confluentinc/kcp/internal/utils"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/zclconf/go-cty/cty"
)

// Variable names for migrate-topics-aiven (Confluent → Aiven replication).
const (
	VarConfluentBootstrapServers  = "confluent_bootstrap_servers"
	VarConfluentSourceAPIKey      = "confluent_source_api_key"
	VarConfluentSourceAPISecret   = "confluent_source_api_secret"
	VarExternalEndpointName       = "external_endpoint_name"
	VarTopics                     = "topics" // list(string) of topic patterns, e.g. [".*"]
	VarMirrormakerServiceName     = "mirrormaker_service_name"
	VarMirrormakerCloudName       = "mirrormaker_cloud_name"
	VarMirrormakerPlan            = "mirrormaker_plan"
	VarAivenKafkaServiceName      = "aiven_kafka_service_name"
)

// Default cluster aliases for replication flow.
const (
	DefaultSourceClusterAlias = "confluent-source"
	DefaultTargetClusterAlias = "aiven-target"
)

// GenerateExternalKafkaEndpoint creates an aiven_service_integration_endpoint for an external
// Kafka cluster (e.g. Confluent Cloud). Uses SASL_SSL with username/password from variables.
// projectVarName, endpointNameVarName, bootstrapServersVarName, apiKeyVarName, apiSecretVarName are variable names.
func GenerateExternalKafkaEndpoint(tfResourceName, projectVarName, endpointNameVarName, bootstrapServersVarName, apiKeyVarName, apiSecretVarName string) *hclwrite.Block {
	block := hclwrite.NewBlock("resource", []string{"aiven_service_integration_endpoint", tfResourceName})
	body := block.Body()

	body.SetAttributeRaw("project", utils.TokensForVarReference(projectVarName))
	body.SetAttributeValue("endpoint_type", cty.StringVal("external_kafka"))
	body.SetAttributeRaw("endpoint_name", utils.TokensForVarReference(endpointNameVarName))

	userConfig := body.AppendNewBlock("external_kafka_user_config", nil)
	uc := userConfig.Body()
	uc.SetAttributeRaw("bootstrap_servers", utils.TokensForVarReference(bootstrapServersVarName))
	uc.SetAttributeValue("security_protocol", cty.StringVal("SASL_SSL"))
	uc.SetAttributeRaw("sasl_username", utils.TokensForVarReference(apiKeyVarName))
	uc.SetAttributeRaw("sasl_password", utils.TokensForVarReference(apiSecretVarName))
	uc.SetAttributeValue("sasl_mechanism", cty.StringVal("PLAIN"))

	return block
}
