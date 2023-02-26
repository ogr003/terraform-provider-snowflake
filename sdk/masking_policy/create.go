package masking_policy

import (
	"context"
	"fmt"
	"strings"
)

type CreateOptions struct {
	Options

	ValueDataType     string
	ReturnDataType    string
	MaskingExpression string
	Comment           *string
}

func (o CreateOptions) validate() error {
	if err := o.Options.validate(); err != nil {
		return fmt.Errorf("validate options: %w", err)
	}
	if o.ValueDataType == "" {
		return fmt.Errorf("value data type must not be empty")
	}
	if o.ReturnDataType == "" {
		return fmt.Errorf("return data type must not be empty")
	}
	if o.MaskingExpression == "" {
		return fmt.Errorf("masking expression must not be empty")
	}
	return nil
}

func (o CreateOptions) build() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf(`CREATE %s %s AS (VAL %s) RETURNS %s -> `, ResourceMaskingPolicy, QualifiedName(o.Name, o.Database, o.Schema), o.ValueDataType, o.ReturnDataType))
	b.WriteString(o.MaskingExpression)
	if o.Comment != nil && *o.Comment != "" {
		b.WriteString(fmt.Sprintf(` COMMENT = '%s'`, *o.Comment))
	}
	return b.String()
}

func (m *maskingPolicies) Create(ctx context.Context, o CreateOptions) (*MaskingPolicy, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate create options: %w", err)
	}
	if _, err := m.client.Exec(ctx, o.build()); err != nil {
		return nil, fmt.Errorf("db exec: %w", err)
	}
	return m.Read(ctx, Options{Name: o.Name, Schema: o.Schema, Database: o.Database})
}
