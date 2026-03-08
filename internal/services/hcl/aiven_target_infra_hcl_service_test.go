package hcl

import (
	"strings"
	"testing"

	"github.com/confluentinc/kcp/internal/types"
)

func TestAivenTargetInfraHCLService_GenerateTerraformFiles(t *testing.T) {
	svc := NewAivenTargetInfraHCLService()
	request := types.AivenTargetRequest{
		ProjectName:    "my-project",
		CloudName:      "aws-eu-west-1",
		Plan:           "business-4",
		ServiceName:    "my-kafka",
		PreventDestroy: true,
	}

	project := svc.GenerateTerraformFiles(request)

	if project.MainTf == "" {
		t.Error("MainTf should not be empty")
	}
	if !strings.Contains(project.MainTf, "aiven_kafka") {
		t.Error("MainTf should contain aiven_kafka resource")
	}
	if !strings.Contains(project.MainTf, "service_name") {
		t.Error("MainTf should reference service_name variable")
	}

	if project.ProvidersTf == "" {
		t.Error("ProvidersTf should not be empty")
	}
	if !strings.Contains(project.ProvidersTf, "aiven") {
		t.Error("ProvidersTf should contain aiven provider")
	}
	if !strings.Contains(project.ProvidersTf, "required_providers") {
		t.Error("ProvidersTf should contain required_providers")
	}

	if project.VariablesTf == "" {
		t.Error("VariablesTf should not be empty")
	}
	if !strings.Contains(project.VariablesTf, "aiven_api_token") {
		t.Error("VariablesTf should define aiven_api_token")
	}
	if !strings.Contains(project.VariablesTf, "project_name") {
		t.Error("VariablesTf should define project_name")
	}

	if project.OutputsTf == "" {
		t.Error("OutputsTf should not be empty")
	}
	if !strings.Contains(project.OutputsTf, "service_uri") {
		t.Error("OutputsTf should include service_uri")
	}

	if project.InputsAutoTfvars == "" {
		t.Error("InputsAutoTfvars should not be empty")
	}
	if !strings.Contains(project.InputsAutoTfvars, "my-project") {
		t.Error("InputsAutoTfvars should contain project value")
	}
	if strings.Contains(project.InputsAutoTfvars, "aiven_api_token") {
		t.Error("InputsAutoTfvars should not contain aiven_api_token (sensitive)")
	}

	if project.ReadmeMd == "" {
		t.Error("ReadmeMd should not be empty")
	}
	if len(project.Modules) != 0 {
		t.Errorf("expected no modules, got %d", len(project.Modules))
	}
}
