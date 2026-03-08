package hcl

import (
	"github.com/confluentinc/kcp/internal/services/hcl/aiven"
	"github.com/confluentinc/kcp/internal/types"
	"github.com/confluentinc/kcp/internal/utils"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/zclconf/go-cty/cty"
)

// AivenTargetInfraHCLService generates Terraform for Aiven for Kafka target infrastructure (Phase 2).
// It produces a single root-level configuration (no submodules): provider + aiven_kafka.
type AivenTargetInfraHCLService struct {
	KafkaResourceName string
}

// NewAivenTargetInfraHCLService returns a service with default resource names.
func NewAivenTargetInfraHCLService() *AivenTargetInfraHCLService {
	return &AivenTargetInfraHCLService{
		KafkaResourceName: aiven.DefaultKafkaResourceName,
	}
}

// GetAivenTargetVariableDefinitions returns all root-level variable definitions for Aiven target infra.
func GetAivenTargetVariableDefinitions() []types.TerraformVariable {
	defs := append([]types.TerraformVariable{}, aiven.AivenProviderVariables...)
	defs = append(defs,
		types.TerraformVariable{Name: aiven.VarProjectName, Description: "Aiven project name", Sensitive: false, Type: "string"},
		types.TerraformVariable{Name: aiven.VarCloudName, Description: "Aiven cloud and region (e.g. google-europe-west1, aws-eu-west-1)", Sensitive: false, Type: "string"},
		types.TerraformVariable{Name: aiven.VarPlan, Description: "Aiven Kafka plan (e.g. business-4, startup-2)", Sensitive: false, Type: "string"},
		types.TerraformVariable{Name: aiven.VarServiceName, Description: "Unique name for the Kafka service", Sensitive: false, Type: "string"},
	)
	return defs
}

// GetAivenTargetOutputDefinitions returns output definitions for the Aiven Kafka service.
func GetAivenTargetOutputDefinitions(kafkaResourceName string) []types.TerraformOutput {
	ref := "aiven_kafka." + kafkaResourceName
	return []types.TerraformOutput{
		{Name: "service_name", Description: "Aiven Kafka service name", Sensitive: false, Value: ref + ".service_name"},
		{Name: "service_host", Description: "Kafka bootstrap host", Sensitive: false, Value: ref + ".service_host"},
		{Name: "service_port", Description: "Kafka service port", Sensitive: false, Value: ref + ".service_port"},
		{Name: "service_uri", Description: "Kafka connection URI (includes credentials)", Sensitive: true, Value: ref + ".service_uri"},
	}
}

// GenerateTerraformFiles produces a full Terraform project for Aiven target infra.
func (s *AivenTargetInfraHCLService) GenerateTerraformFiles(request types.AivenTargetRequest) types.MigrationInfraTerraformProject {
	return types.MigrationInfraTerraformProject{
		MainTf:           s.generateRootMainTf(request),
		ProvidersTf:      s.generateRootProvidersTf(),
		VariablesTf:      s.generateVariablesTf(GetAivenTargetVariableDefinitions()),
		OutputsTf:        s.generateOutputsTf(GetAivenTargetOutputDefinitions(s.KafkaResourceName)),
		InputsAutoTfvars: s.generateInputsAutoTfvars(request),
		ReadmeMd:         s.generateReadmeMd(),
		Modules:          nil,
	}
}

func (s *AivenTargetInfraHCLService) generateRootMainTf(request types.AivenTargetRequest) string {
	f := hclwrite.NewEmptyFile()
	rootBody := f.Body()

	rootBody.AppendBlock(aiven.GenerateKafkaResource(
		s.KafkaResourceName,
		aiven.VarProjectName,
		aiven.VarCloudName,
		aiven.VarPlan,
		aiven.VarServiceName,
		request.PreventDestroy,
	))

	return string(f.Bytes())
}

func (s *AivenTargetInfraHCLService) generateRootProvidersTf() string {
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

func (s *AivenTargetInfraHCLService) generateVariablesTf(tfVariables []types.TerraformVariable) string {
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

func (s *AivenTargetInfraHCLService) generateOutputsTf(outputs []types.TerraformOutput) string {
	f := hclwrite.NewEmptyFile()
	rootBody := f.Body()

	for _, o := range outputs {
		block := rootBody.AppendNewBlock("output", []string{o.Name})
		body := block.Body()
		body.SetAttributeRaw("value", utils.TokensForResourceReference(o.Value))
		if o.Description != "" {
			body.SetAttributeValue("description", cty.StringVal(o.Description))
		}
		body.SetAttributeValue("sensitive", cty.BoolVal(o.Sensitive))
		rootBody.AppendNewline()
	}

	return string(f.Bytes())
}

// generateInputsAutoTfvars sets only non-sensitive variables so that api_token is not written to disk.
// Users should set AIVEN_TOKEN when running terraform apply or use -var="aiven_api_token=...".
func (s *AivenTargetInfraHCLService) generateInputsAutoTfvars(request types.AivenTargetRequest) string {
	f := hclwrite.NewEmptyFile()
	rootBody := f.Body()

	rootBody.SetAttributeValue("project_name", cty.StringVal(request.ProjectName))
	rootBody.SetAttributeValue("cloud_name", cty.StringVal(request.CloudName))
	rootBody.SetAttributeValue("plan", cty.StringVal(request.Plan))
	rootBody.SetAttributeValue("service_name", cty.StringVal(request.ServiceName))

	return string(f.Bytes())
}

func (s *AivenTargetInfraHCLService) generateReadmeMd() string {
	return "# Aiven for Kafka Target Infrastructure\n\n" +
		"This Terraform configuration creates an Aiven for Apache Kafka® service for use as a migration target (e.g. Confluent Cloud → Aiven).\n\n" +
		"## Prerequisites\n\n" +
		"- An [Aiven](https://aiven.io) account and a project.\n" +
		"- [Aiven API token](https://help.aiven.io/en/articles/2059201-authentication-tokens) with permissions to create services.\n\n" +
		"## Authentication\n\n" +
		"The Aiven Terraform provider requires an API token. Use one of:\n\n" +
		"1. **Environment variable (recommended):** Set AIVEN_TOKEN before running terraform apply.\n" +
		"2. **Variable:** Run with -var=\"aiven_api_token=YOUR_TOKEN\" (avoid storing in version control).\n" +
		"3. **KCP credentials file:** For KCP CLI, use aiven-credentials.yaml or AIVEN_TOKEN / AIVEN_API_TOKEN; see docs/aiven-credentials.example.yaml.\n\n" +
		"Do not commit aiven_api_token to inputs.auto.tfvars. The generated inputs.auto.tfvars contains only non-sensitive parameters.\n\n" +
		"## Variables\n\n" +
		"| Variable | Description |\n" +
		"|----------|-------------|\n" +
		"| aiven_api_token | Aiven API token (sensitive). Set via env or -var. |\n" +
		"| project_name | Aiven project name. |\n" +
		"| cloud_name | Cloud and region (e.g. google-europe-west1, aws-eu-west-1). |\n" +
		"| plan | Service plan (e.g. business-4, startup-2). |\n" +
		"| service_name | Unique name for the Kafka service. |\n\n" +
		"## Outputs\n\n" +
		"After terraform apply, use the outputs to connect clients or configure MirrorMaker 2:\n\n" +
		"- service_uri — Connection URI (includes credentials; sensitive).\n" +
		"- service_host, service_port — Bootstrap host and port.\n\n" +
		"Kafka SASL credentials (username/password) are provided by Aiven for the service; create service users in the Aiven Console or via API and use them for MirrorMaker 2 and clients.\n"
}
