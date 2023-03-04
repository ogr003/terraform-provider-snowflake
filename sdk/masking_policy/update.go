package masking_policy

import (
	"context"
	"fmt"
	"strings"
)

type UpdateOptions struct {
	Options

	Comment           *string
	RemoveComment     *bool
	MaskingExpression *string
}

func (o UpdateOptions) build() string {
	var b strings.Builder
	if o.RemoveComment != nil && *o.RemoveComment {
		b.WriteString(` UNSET COMMENT`)
	} else if o.Comment != nil {
		b.WriteString(fmt.Sprintf(` SET COMMENT = '%s'`, *o.Comment))
	} else if o.MaskingExpression != nil {
		b.WriteString(fmt.Sprintf(` SET BODY -> %s`, *o.MaskingExpression))
	}
	return b.String()
}

func (m *maskingPolicies) Update(ctx context.Context, o UpdateOptions) (*MaskingPolicy, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate update options: %w", err)
	}
	stmt := fmt.Sprintf(`ALTER %s "%s" %s`, ResourceMaskingPolicy, QualifiedName(o.Name, o.Database, o.Schema), o.build())
	if _, err := m.client.Exec(ctx, stmt); err != nil {
		return nil, fmt.Errorf("db exec: %w", err)
	}
	return m.Read(ctx, Options{Name: o.Name, Database: o.Database, Schema: o.Schema})
}
