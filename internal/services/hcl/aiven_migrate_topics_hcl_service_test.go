package hcl

import (
	"strings"
	"testing"

	"github.com/confluentinc/kcp/internal/types"
)

func TestAivenMigrateTopicsHCLService_GenerateTerraformFiles(t *testing.T) {
	svc := NewAivenMigrateTopicsHCLService()
	request := types.AivenMigrateTopicsRequest{
		ProjectName:               "my-project",
		AivenKafkaServiceName:     "my-kafka",
		MirrorMakerServiceName:    "my-mm2",
		MirrorMakerCloudName:      "aws-eu-west-1",
		MirrorMakerPlan:           "business-4",
		ConfluentBootstrapServers: "pkc-xxx.eu-west-1.aws.confluent.cloud:9092",
		TopicPattern:             ".*",
		PreventDestroy:            true,
	}

	project := svc.GenerateTerraformFiles(request)

	if project.MainTf == "" {
		t.Fatal("MainTf should not be empty")
	}
	if !strings.Contains(project.MainTf, "aiven_service_integration_endpoint") {
		t.Error("MainTf should contain external Kafka endpoint")
	}
	if !strings.Contains(project.MainTf, "aiven_kafka_mirrormaker") {
		t.Error("MainTf should contain MirrorMaker 2 resource")
	}
	if !strings.Contains(project.MainTf, "aiven_mirrormaker_replication_flow") {
		t.Error("MainTf should contain replication flow")
	}
	if !strings.Contains(project.MainTf, "SASL_SSL") {
		t.Error("MainTf should use SASL_SSL for Confluent")
	}

	if project.ProvidersTf == "" {
		t.Fatal("ProvidersTf should not be empty")
	}
	if project.VariablesTf == "" {
		t.Fatal("VariablesTf should not be empty")
	}
	if !strings.Contains(project.VariablesTf, "confluent_source_api_key") {
		t.Error("VariablesTf should define Confluent API key variable")
	}
	if project.InputsAutoTfvars == "" {
		t.Fatal("InputsAutoTfvars should not be empty")
	}
	if strings.Contains(project.InputsAutoTfvars, "confluent_source_api_secret") {
		t.Error("InputsAutoTfvars should not contain Confluent secret")
	}
	if project.ReadmeMd == "" {
		t.Fatal("ReadmeMd should not be empty")
	}
	if !strings.Contains(project.ReadmeMd, "SASL") {
		t.Error("ReadmeMd should document SASL/ACL requirements")
	}
	if !strings.Contains(project.MainTf, "depends_on") {
		t.Error("MainTf replication flow should include depends_on for service integrations")
	}
}
