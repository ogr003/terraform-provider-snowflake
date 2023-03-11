package view

import (
	"context"
	"fmt"
	"strings"

	"github.com/snowflakedb/terraform-provider-snowflake/sdk/tag"
	"github.com/snowflakedb/terraform-provider-snowflake/sdk/utils"
)

type UpdateOptions struct {
	Options

	Comment       *string
	RemoveComment *bool
	Secure        *bool
	AddTag        *tag.Value
	UnsetTag      *tag.Value
	ChangeTag     *tag.Value
}

func (o UpdateOptions) build() string {
	var b strings.Builder
	if o.RemoveComment != nil && *o.RemoveComment {
		b.WriteString(" UNSET COMMENT")
	} else if o.Comment != nil {
		b.WriteString(fmt.Sprintf(" SET COMMENT = '%s'", utils.EscapeString(*o.Comment)))
	} else if o.AddTag != nil {
		b.WriteString(fmt.Sprintf(` SET TAG "%s"."%s"."%s" = "%s"`, o.AddTag.Database, o.AddTag.Schema, o.AddTag.Name, o.AddTag.Value))
	} else if o.UnsetTag != nil {
		b.WriteString(fmt.Sprintf(` UNSET TAG "%s"."%s"."%s"`, o.UnsetTag.Database, o.UnsetTag.Schema, o.UnsetTag.Name))
	} else if o.ChangeTag != nil {
		b.WriteString(fmt.Sprintf(` SET TAG "%s"."%s"."%s" = "%s"`, o.ChangeTag.Database, o.ChangeTag.Schema, o.ChangeTag.Name, o.ChangeTag.Value))
	} else if o.Secure != nil {
		if *o.Secure {
			b.WriteString(" SET SECURE")
		} else {
			b.WriteString(" UNSET SECURE")
		}
	}
	return b.String()
}

func (v *views) Update(ctx context.Context, o UpdateOptions) (*View, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate update options: %w", err)
	}
	stmt := fmt.Sprintf(`ALTER %s "%s" %s`, ResourceView, QualifiedName(o.Database, o.Schema, o.Name), o.build())
	if _, err := v.client.Exec(ctx, stmt); err != nil {
		return nil, fmt.Errorf("db exec: %w", err)
	}
	return v.Read(ctx, Options{Name: o.Name, Schema: o.Schema, Database: o.Database})
}
