package hcl

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/confluentinc/kcp/internal/services/hcl/aiven"
	"github.com/confluentinc/kcp/internal/types"
	"github.com/confluentinc/kcp/internal/utils"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/zclconf/go-cty/cty"
)

// AivenMigrateAclsHCLService generates Terraform for Aiven Kafka ACLs (Phase 2.3).
// Only topic-level ACLs are emitted; Aiven ACLs are topic-scoped (read, write, readwrite, admin).
type AivenMigrateAclsHCLService struct{}

// NewAivenMigrateAclsHCLService returns a new service.
func NewAivenMigrateAclsHCLService() *AivenMigrateAclsHCLService {
	return &AivenMigrateAclsHCLService{}
}

// TopicACLEntry is a collapsed (principal, topic, permission) for Aiven.
type TopicACLEntry struct {
	Username   string
	Topic      string
	Permission string // admin, read, write, readwrite
}

// CollapseTopicACLs converts Kafka ACLs (by principal) to Aiven topic ACL entries.
// Only ResourceType TOPIC is considered. Multiple operations on the same (principal, topic) are collapsed
// to one permission: both Read+Write -> readwrite, Read -> read, Write -> write, else -> admin.
func CollapseTopicACLs(aclsByPrincipal map[string][]types.Acls) []TopicACLEntry {
	type key struct{ user, topic string }
	perms := make(map[key]map[string]bool) // set of operations per (user, topic)

	for principal, acls := range aclsByPrincipal {
		user := stripPrincipal(principal)
		if user == "" {
			user = principal
		}
		for _, acl := range acls {
			if strings.ToUpper(acl.ResourceType) != "TOPIC" {
				continue
			}
			k := key{user, acl.ResourceName}
			if perms[k] == nil {
				perms[k] = make(map[string]bool)
			}
			op := strings.ToUpper(acl.Operation)
			perms[k][op] = true
		}
	}

	var result []TopicACLEntry
	for k, ops := range perms {
		perm := aivenPermission(ops)
		result = append(result, TopicACLEntry{Username: k.user, Topic: k.topic, Permission: perm})
	}
	return result
}

func stripPrincipal(principal string) string {
	s := strings.TrimSpace(principal)
	if strings.HasPrefix(strings.ToUpper(s), "USER:") {
		return strings.TrimSpace(s[5:])
	}
	return s
}

func aivenPermission(ops map[string]bool) string {
	hasRead := ops["READ"]
	hasWrite := ops["WRITE"]
	if hasRead && hasWrite {
		return "readwrite"
	}
	if hasRead {
		return "read"
	}
	if hasWrite {
		return "write"
	}
	return "admin"
}

// GetAivenMigrateAclsVariableDefinitions returns variable definitions for migrate-acls-aiven.
func GetAivenMigrateAclsVariableDefinitions() []types.TerraformVariable {
	return []types.TerraformVariable{
		{Name: aiven.VarAivenAPIToken, Description: "Aiven API token", Sensitive: true, Type: "string"},
		{Name: aiven.VarProjectName, Description: "Aiven project name", Sensitive: false, Type: "string"},
		{Name: aiven.VarAivenKafkaServiceName, Description: "Aiven Kafka service name", Sensitive: false, Type: "string"},
	}
}

// GenerateTerraformFiles produces a Terraform project for Aiven Kafka ACLs.
func (s *AivenMigrateAclsHCLService) GenerateTerraformFiles(request types.AivenMigrateAclsRequest) types.MigrationInfraTerraformProject {
	return types.MigrationInfraTerraformProject{
		MainTf:           s.generateMainTf(request),
		ProvidersTf:      s.generateProvidersTf(),
		VariablesTf:      s.generateVariablesTf(),
		OutputsTf:        s.generateOutputsTf(),
		InputsAutoTfvars: s.generateInputsAutoTfvars(request),
		ReadmeMd:         s.generateReadmeMd(),
		Modules:          nil,
	}
}

func (s *AivenMigrateAclsHCLService) generateMainTf(request types.AivenMigrateAclsRequest) string {
	f := hclwrite.NewEmptyFile()
	rootBody := f.Body()

	entries := CollapseTopicACLs(request.AclsByPrincipal)
	if len(entries) == 0 {
		rootBody.AppendUnstructuredTokens(utils.TokensForComment("// No topic-level ACLs to migrate; add aiven_kafka_acl resources as needed."))
		rootBody.AppendNewline()
	}
	for i, e := range entries {
		tfResourceName := "acl_" + utils.FormatHclResourceName(e.Username) + "_" + utils.FormatHclResourceName(e.Topic) + "_" + strconv.Itoa(i)
		rootBody.AppendBlock(aiven.GenerateKafkaACL(tfResourceName, aiven.VarProjectName, aiven.VarAivenKafkaServiceName, e.Topic, e.Permission, e.Username))
		rootBody.AppendNewline()
	}

	return string(f.Bytes())
}

func (s *AivenMigrateAclsHCLService) generateProvidersTf() string {
	f := hclwrite.NewEmptyFile()
	rootBody := f.Body()
	tfBlock := rootBody.AppendNewBlock("terraform", nil)
	tfBlock.Body().AppendNewBlock("required_providers", nil).Body().SetAttributeRaw(aiven.GenerateRequiredProviderTokens())
	rootBody.AppendNewline()
	rootBody.AppendBlock(aiven.GenerateProviderBlock())
	return string(f.Bytes())
}

func (s *AivenMigrateAclsHCLService) generateVariablesTf() string {
	f := hclwrite.NewEmptyFile()
	rootBody := f.Body()
	for _, v := range GetAivenMigrateAclsVariableDefinitions() {
		vb := rootBody.AppendNewBlock("variable", []string{v.Name}).Body()
		vb.SetAttributeRaw("type", utils.TokensForResourceReference(v.Type))
		if v.Description != "" {
			vb.SetAttributeValue("description", cty.StringVal(v.Description))
		}
		if v.Sensitive {
			vb.SetAttributeValue("sensitive", cty.BoolVal(true))
		}
		rootBody.AppendNewline()
	}
	return string(f.Bytes())
}

func (s *AivenMigrateAclsHCLService) generateOutputsTf() string {
	f := hclwrite.NewEmptyFile()
	// No outputs required
	return string(f.Bytes())
}

func (s *AivenMigrateAclsHCLService) generateInputsAutoTfvars(request types.AivenMigrateAclsRequest) string {
	f := hclwrite.NewEmptyFile()
	rootBody := f.Body()
	rootBody.SetAttributeValue("project_name", cty.StringVal(request.ProjectName))
	rootBody.SetAttributeValue(aiven.VarAivenKafkaServiceName, cty.StringVal(request.AivenKafkaServiceName))
	return string(f.Bytes())
}

func (s *AivenMigrateAclsHCLService) generateReadmeMd() string {
	return "# Aiven Kafka ACLs (migrated)\n\n" +
		"This Terraform manages Kafka ACLs on your Aiven Kafka service. Generated by `kcp create-asset migrate-acls-aiven`.\n\n" +
		"## Prerequisites\n\n" +
		"- Aiven Kafka service created (e.g. from `target-infra-aiven`).\n" +
		"- ACL input file in the format expected by migrate-acls-aiven (JSON array of ACL objects with Principal, ResourceType, ResourceName, Operation, PermissionType).\n\n" +
		"## Limitations\n\n" +
		"- Only **topic-level** ACLs are generated. Aiven ACLs are topic-scoped (read, write, readwrite, admin).\n" +
		"- Group, Cluster, and TransactionalId ACLs from the source are not converted; document them separately if needed.\n" +
		"- Principal format: `User:name` is normalized to `name` for the Aiven username.\n\n" +
		"## Variables\n\n" +
		"| Variable | Description |\n" +
		"|----------|-------------|\n" +
		"| aiven_api_token | Aiven API token (sensitive) |\n" +
		"| project_name | Aiven project |\n" +
		"| " + aiven.VarAivenKafkaServiceName + " | Aiven Kafka service name |\n"
}

// GenerateTerraformFilesFromACLs is a convenience that builds AivenMigrateAclsRequest from aclsByPrincipal.
func GenerateTerraformFilesFromACLs(projectName, serviceName string, aclsByPrincipal map[string][]types.Acls) (types.MigrationInfraTerraformProject, error) {
	if projectName == "" || serviceName == "" {
		return types.MigrationInfraTerraformProject{}, fmt.Errorf("project_name and aiven_kafka_service_name are required")
	}
	req := types.AivenMigrateAclsRequest{
		ProjectName:           projectName,
		AivenKafkaServiceName: serviceName,
		AclsByPrincipal:       aclsByPrincipal,
	}
	svc := NewAivenMigrateAclsHCLService()
	return svc.GenerateTerraformFiles(req), nil
}
