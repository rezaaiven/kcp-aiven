package aiven

import (
	"github.com/confluentinc/kcp/internal/utils"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/zclconf/go-cty/cty"
)

// GenerateServiceIntegrationEndpointToMirrormaker creates an aiven_service_integration that
// connects an external Kafka endpoint (e.g. Confluent) to the MirrorMaker 2 service as a source.
// sourceEndpointIdRef is the resource reference (e.g. "aiven_service_integration_endpoint.confluent.id").
// destinationServiceNameVarName is the variable name for the MM2 service name.
// clusterAlias is the alias used in replication flows (e.g. "confluent-source").
func GenerateServiceIntegrationEndpointToMirrormaker(tfResourceName, sourceEndpointIdRef, destinationServiceNameVarName, clusterAlias string) *hclwrite.Block {
	block := hclwrite.NewBlock("resource", []string{"aiven_service_integration", tfResourceName})
	body := block.Body()

	body.SetAttributeRaw("source_endpoint_id", utils.TokensForResourceReference(sourceEndpointIdRef))
	body.SetAttributeRaw("destination_service_name", utils.TokensForVarReference(destinationServiceNameVarName))
	body.SetAttributeValue("integration_type", cty.StringVal("kafka_mirrormaker"))

	userConfig := body.AppendNewBlock("kafka_mirrormaker_user_config", nil)
	userConfig.Body().SetAttributeValue("cluster_alias", cty.StringVal(clusterAlias))

	return block
}

// GenerateServiceIntegrationKafkaToMirrormaker creates an aiven_service_integration that
// connects an Aiven Kafka service to the MirrorMaker 2 service (as target).
// sourceServiceNameVarName is the variable name for the Aiven Kafka service name.
func GenerateServiceIntegrationKafkaToMirrormaker(tfResourceName, sourceServiceNameVarName, destinationServiceNameVarName, clusterAlias string) *hclwrite.Block {
	block := hclwrite.NewBlock("resource", []string{"aiven_service_integration", tfResourceName})
	body := block.Body()

	body.SetAttributeRaw("source_service_name", utils.TokensForVarReference(sourceServiceNameVarName))
	body.SetAttributeRaw("destination_service_name", utils.TokensForVarReference(destinationServiceNameVarName))
	body.SetAttributeValue("integration_type", cty.StringVal("kafka_mirrormaker"))

	userConfig := body.AppendNewBlock("kafka_mirrormaker_user_config", nil)
	userConfig.Body().SetAttributeValue("cluster_alias", cty.StringVal(clusterAlias))

	return block
}
