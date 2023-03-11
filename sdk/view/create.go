package view

import (
	"context"
	"fmt"
	"strings"

	"github.com/snowflakedb/terraform-provider-snowflake/sdk/utils"
)

type CreateOptions struct {
	Options

	Statement string
	Replace   *bool
	Secure    *bool
	Comment   *string
}

func (o CreateOptions) validate() error {
	if err := o.Options.validate(); err != nil {
		return fmt.Errorf("validate options: %w", err)
	}
	if o.Statement == "" {
		return fmt.Errorf("statement is required")
	}
	return nil
}

func (o CreateOptions) build() string {
	var b strings.Builder

	b.WriteString("CREATE")
	if o.Replace != nil && *o.Replace {
		b.WriteString(" OR REPLACE")
	}
	if o.Secure != nil && *o.Secure {
		b.WriteString(" SECURE")
	}
	b.WriteString(fmt.Sprintf(` %s %s`, ResourceView, QualifiedName(o.Database, o.Schema, o.Name)))
	if o.Comment != nil && *o.Comment != "" {
		b.WriteString(fmt.Sprintf(` COMMENT = '%s'`, utils.EscapeString(*o.Comment)))
	}
	b.WriteString(fmt.Sprintf(` AS %s`, o.Statement))
	return b.String()
}

func (v *views) Create(ctx context.Context, options CreateOptions) (*View, error) {
	if err := options.validate(); err != nil {
		return nil, fmt.Errorf("validate create options: %w", err)
	}
	if _, err := v.client.Exec(ctx, options.build()); err != nil {
		return nil, fmt.Errorf("db exec: %w", err)
	}
	return v.Read(ctx, Options{Name: options.Name, Database: options.Database, Schema: options.Schema})
}
