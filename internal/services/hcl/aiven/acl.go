package aiven

import (
	"github.com/confluentinc/kcp/internal/utils"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/zclconf/go-cty/cty"
)

// GenerateKafkaACL creates an aiven_kafka_acl resource (topic-level ACL).
// projectVarName and serviceNameVarName are Terraform variable names.
// topic is the topic name (or pattern); permission is one of "admin", "read", "write", "readwrite".
// username is the principal (e.g. service account name or "User:xxx" stripped to "xxx").
func GenerateKafkaACL(tfResourceName, projectVarName, serviceNameVarName, topic, permission, username string) *hclwrite.Block {
	block := hclwrite.NewBlock("resource", []string{"aiven_kafka_acl", tfResourceName})
	body := block.Body()
	body.SetAttributeRaw("project", utils.TokensForVarReference(projectVarName))
	body.SetAttributeRaw("service_name", utils.TokensForVarReference(serviceNameVarName))
	body.SetAttributeValue("topic", cty.StringVal(topic))
	body.SetAttributeValue("permission", cty.StringVal(permission))
	body.SetAttributeValue("username", cty.StringVal(username))
	return block
}
