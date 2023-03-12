package utils

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

type emptyDescriptions struct {
}

func (emptyDescriptions) Description(ctx context.Context) string {
	return ""
}

func (emptyDescriptions) MarkdownDescription(ctx context.Context) string {
	return ""
}

var errorNotFoundRegexp = regexp.MustCompile("NOT_FOUND|does not exist")

func isNotFoundError(err error) bool {
	return errorNotFoundRegexp.MatchString(err.Error())
}

type CaseInsensitive struct {
	emptyDescriptions
}

func (CaseInsensitive) Modify(ctx context.Context, req tfsdk.ModifyAttributePlanRequest, resp *tfsdk.ModifyAttributePlanResponse) {
	if req.AttributeState == nil {
		return
	}
	state := req.AttributeState.(types.String)
	plan := req.AttributePlan.(types.String)
	if strings.EqualFold(state.Value, plan.Value) {
		resp.AttributePlan = state
	} else {
		resp.AttributePlan = plan
	}
}

func ErrorConvertingProvider(typ interface{}) diag.ErrorDiagnostic {
	return diag.NewErrorDiagnostic(
		"Error converting provider",
		fmt.Sprintf("An unexpected error was encountered converting the provider. This is always a bug in the provider.\n\nType: %T", typ),
	)
}
