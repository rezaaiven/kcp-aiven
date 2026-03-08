package aiven

import (
	"github.com/confluentinc/kcp/internal/utils"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/zclconf/go-cty/cty"
)

const (
	// DefaultKafkaResourceName is the Terraform resource name for the generated aiven_kafka service.
	DefaultKafkaResourceName = "kafka"
)

// Var names for Aiven target infra (used in variables.tf and main.tf).
const (
	VarProjectName   = "project_name"
	VarCloudName     = "cloud_name"
	VarPlan          = "plan"
	VarServiceName   = "service_name"
)

// GenerateKafkaResource creates an aiven_kafka resource block.
// projectVarName, cloudNameVarName, planVarName, serviceNameVarName are Terraform variable names (e.g. "project_name").
func GenerateKafkaResource(tfResourceName, projectVarName, cloudNameVarName, planVarName, serviceNameVarName string, preventDestroy bool) *hclwrite.Block {
	block := hclwrite.NewBlock("resource", []string{"aiven_kafka", tfResourceName})
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

// GenerateKafkaTopic creates an aiven_kafka_topic resource (optional explicit topic creation on Aiven).
// projectVarName and serviceNameVarName are Terraform variable names; topicName is the Kafka topic name.
func GenerateKafkaTopic(tfResourceName, projectVarName, serviceNameVarName, topicName string) *hclwrite.Block {
	block := hclwrite.NewBlock("resource", []string{"aiven_kafka_topic", tfResourceName})
	body := block.Body()
	body.SetAttributeRaw("project", utils.TokensForVarReference(projectVarName))
	body.SetAttributeRaw("service_name", utils.TokensForVarReference(serviceNameVarName))
	body.SetAttributeValue("topic_name", cty.StringVal(topicName))
	return block
}
