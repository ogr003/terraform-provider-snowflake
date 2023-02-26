package tag

import (
	"context"
	"fmt"
	"strings"

	"github.com/snowflakedb/terraform-provider-snowflake/sdk/utils"
)

type UpdateOptions struct {
	Options

	Comment             *string
	RemoveComment       *bool
	AddAllowedValues    *[]string
	DropAllowedValue    *[]string
	RemoveAllowedValues *bool
	MaskingPolicy       *string
	RemoveMaskingPolicy *bool
}

func (o UpdateOptions) build() string {
	var b strings.Builder
	if o.RemoveComment != nil && *o.RemoveComment {
		b.WriteString(` UNSET COMMENT`)
	} else if o.AddAllowedValues != nil && len(*o.AddAllowedValues) > 0 {
		b.WriteString(fmt.Sprintf(` ADD ALLOWED_VALUES %s`, utils.ListToSnowflakeString(*o.AddAllowedValues)))
	} else if o.DropAllowedValue != nil && len(*o.DropAllowedValue) > 0 {
		b.WriteString(fmt.Sprintf(` DROP ALLOWED_VALUES %s`, utils.ListToSnowflakeString(*o.DropAllowedValue)))
	} else if o.RemoveAllowedValues != nil && *o.RemoveAllowedValues {
		b.WriteString(` UNSET ALLOWED_VALUES`)
	} else if o.Comment != nil {
		b.WriteString(fmt.Sprintf(` SET COMMENT = '%s'`, utils.EscapeString(*o.Comment)))
	} else if o.MaskingPolicy != nil {
		if o.RemoveMaskingPolicy != nil && *o.RemoveMaskingPolicy {
			b.WriteString(fmt.Sprintf(` UNSET MASKING_POLICY %s`, *o.MaskingPolicy))
		} else {
			b.WriteString(fmt.Sprintf(` SET MASKING_POLICY %s`, *o.MaskingPolicy))
		}
	}
	return b.String()
}

func (t *tags) Update(ctx context.Context, o UpdateOptions) (*Tag, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate update options: %w", err)
	}
	stmt := fmt.Sprintf(`ALTER %s "%s" %s`, ResourceTag, QualifiedName(o.Name, o.Schema, o.Database), o.build())
	if _, err := t.client.Exec(ctx, stmt); err != nil {
		return nil, fmt.Errorf("db exec: %w", err)
	}
	return t.Read(ctx, Options{Name: o.Name, Database: o.Database, Schema: o.Schema})
}
