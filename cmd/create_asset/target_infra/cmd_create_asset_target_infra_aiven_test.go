package targetinfra

import (
	"testing"
)

func TestPreRunCreateTargetInfraAiven_Validation(t *testing.T) {
	cmd := NewTargetInfraAivenCmd()
	// Bind env so flags get env values when set
	cmd.Flags().Set("project", "myproject")
	cmd.Flags().Set("cloud-name", "aws-eu-west-1")
	cmd.Flags().Set("service-name", "my-kafka")

	err := preRunCreateTargetInfraAiven(cmd, nil)
	if err != nil {
		t.Errorf("expected nil error for valid flags, got %v", err)
	}
}

func TestPreRunCreateTargetInfraAiven_InvalidServiceName(t *testing.T) {
	cmd := NewTargetInfraAivenCmd()
	cmd.Flags().Set("project", "myproject")
	cmd.Flags().Set("cloud-name", "aws-eu-west-1")
	cmd.Flags().Set("service-name", "bad name with spaces")

	err := preRunCreateTargetInfraAiven(cmd, nil)
	if err == nil {
		t.Error("expected error for service name with spaces")
	}
}

func TestPreRunCreateTargetInfraAiven_InvalidPreventDestroy(t *testing.T) {
	cmd := NewTargetInfraAivenCmd()
	cmd.Flags().Set("project", "myproject")
	cmd.Flags().Set("cloud-name", "aws-eu-west-1")
	cmd.Flags().Set("service-name", "my-kafka")
	cmd.Flags().Set("prevent-destroy", "invalid")

	err := preRunCreateTargetInfraAiven(cmd, nil)
	if err == nil {
		t.Error("expected error for invalid prevent-destroy value")
	}
}
