package aiven

import (
	"github.com/confluentinc/kcp/internal/utils"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/zclconf/go-cty/cty"
)

// GenerateKafkaMirrormaker creates an aiven_kafka_mirrormaker resource (MirrorMaker 2 service).
func GenerateKafkaMirrormaker(tfResourceName, projectVarName, cloudNameVarName, planVarName, serviceNameVarName string, preventDestroy bool) *hclwrite.Block {
	block := hclwrite.NewBlock("resource", []string{"aiven_kafka_mirrormaker", tfResourceName})
	body := block.Body()

	body.SetAttributeRaw("project", utils.TokensForVarReference(projectVarName))
	body.SetAttributeRaw("cloud_name", utils.TokensForVarReference(cloudNameVarName))
	body.SetAttributeRaw("plan", utils.TokensForVarReference(planVarName))
	body.SetAttributeRaw("service_name", utils.TokensForVarReference(serviceNameVarName))

	if preventDestroy {
		_ = utils.GenerateLifecycleBlock(block, "prevent_destroy", true)
	}

	return block
}

// GenerateMirrormakerReplicationFlow creates an aiven_mirrormaker_replication_flow resource.
// sourceClusterAlias and targetClusterAlias are the cluster alias names from the service integrations.
// topicsVarName is the variable name for the list of topic patterns (e.g. "topics" -> var.topics).
// dependsOnRefs are optional resource references so the flow is created after the integrations (e.g. ["aiven_service_integration.confluent_to_mm2", "aiven_service_integration.aiven_kafka_to_mm2"]).
func GenerateMirrormakerReplicationFlow(tfResourceName, projectVarName, mm2ServiceNameVarName, sourceClusterAlias, targetClusterAlias, topicsVarName string, dependsOnRefs []string) *hclwrite.Block {
	block := hclwrite.NewBlock("resource", []string{"aiven_mirrormaker_replication_flow", tfResourceName})
	body := block.Body()

	body.SetAttributeRaw("project", utils.TokensForVarReference(projectVarName))
	body.SetAttributeRaw("service_name", utils.TokensForVarReference(mm2ServiceNameVarName))
	body.SetAttributeValue("source_cluster", cty.StringVal(sourceClusterAlias))
	body.SetAttributeValue("target_cluster", cty.StringVal(targetClusterAlias))
	body.SetAttributeRaw("topics", utils.TokensForVarReference(topicsVarName))

	if len(dependsOnRefs) > 0 {
		body.SetAttributeRaw("depends_on", utils.TokensForList(dependsOnRefs))
	}

	return block
}
