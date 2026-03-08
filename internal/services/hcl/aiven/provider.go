package aiven

import (
	"github.com/confluentinc/kcp/internal/types"
	"github.com/confluentinc/kcp/internal/utils"
	"github.com/hashicorp/hcl/v2/hclwrite"
)

const (
	// VarAivenAPIToken is the Terraform variable name for the Aiven API token.
	VarAivenAPIToken = "aiven_api_token"
)

// AivenProviderVariables are the root-level variables for the Aiven provider.
var AivenProviderVariables = []types.TerraformVariable{
	{Name: VarAivenAPIToken, Description: "Aiven API token (or set AIVEN_TOKEN when running terraform apply)", Sensitive: true, Type: "string"},
}

// GenerateRequiredProviderTokens returns the required_providers entry for Aiven.
func GenerateRequiredProviderTokens() (string, hclwrite.Tokens) {
	aivenProvider := map[string]hclwrite.Tokens{
		"source":  utils.TokensForStringTemplate("aiven/aiven"),
		"version": utils.TokensForStringTemplate("~> 4.0"),
	}
	return "aiven", utils.TokensForMap(aivenProvider)
}

// GenerateProviderBlock returns the Aiven provider block (api_token from variable).
func GenerateProviderBlock() *hclwrite.Block {
	providerBlock := hclwrite.NewBlock("provider", []string{"aiven"})
	providerBlock.Body().SetAttributeRaw("api_token", utils.TokensForVarReference(VarAivenAPIToken))
	return providerBlock
}
