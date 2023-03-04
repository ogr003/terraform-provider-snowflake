package tag

import (
	"context"
	"fmt"
	"strings"

	"github.com/snowflakedb/terraform-provider-snowflake/sdk/utils"
)

type CreateOptions struct {
	Options

	AllowedValues *string
	Comment       *string
}

func (o CreateOptions) build() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf(`CREATE %s "%s"`, ResourceTag, QualifiedName(o.Name, o.Database, o.Schema)))
	if o.AllowedValues != nil && *o.AllowedValues != "" {
		b.WriteString(fmt.Sprintf(` ALLOWED_VALUES %s`, *o.AllowedValues))
	}
	if o.Comment != nil && *o.Comment != "" {
		b.WriteString(fmt.Sprintf(` COMMENT = '%s'`, utils.EscapeString(*o.Comment)))
	}
	return b.String()
}

func (t *tags) Create(ctx context.Context, o CreateOptions) (*Tag, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate create options: %w", err)
	}
	if _, err := t.client.Exec(ctx, o.build()); err != nil {
		return nil, fmt.Errorf("db exec: %w", err)
	}
	return t.Read(ctx, Options{Name: o.Name, Schema: o.Schema, Database: o.Database})
}
