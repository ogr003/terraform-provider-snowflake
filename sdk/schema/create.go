package schema

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/snowflakedb/terraform-provider-snowflake/sdk/utils"
)

// CreateOptions represents the options for creating a schema.
type CreateOptions struct {
	Name              string
	Database          string
	DataRetentionTime *int32
	Transient         *bool
	ManagedAccess     *bool
	Comment           *string
}

func (o CreateOptions) validate() error {
	if o.Name == "" {
		return errors.New("name must not be empty")
	}
	if o.Database == "" {
		return errors.New("database name must not be empty")
	}
	return nil
}

func (o CreateOptions) build() string {
	var b strings.Builder
	b.WriteString("CREATE")
	if o.Transient != nil && *o.Transient {
		b.WriteString(" TRANSIENT")
	}
	b.WriteString(fmt.Sprintf(" %s %s", ResourceSchema, QualifiedName(o.Name, o.Database)))
	if o.ManagedAccess != nil && *o.ManagedAccess {
		b.WriteString(" WITH MANAGED ACCESS")
	}
	if o.DataRetentionTime != nil {
		b.WriteString(fmt.Sprintf(" DATA_RETENTION_TIME_IN_DAYS = %d", *o.DataRetentionTime))
	}
	if o.Comment != nil && *o.Comment != "" {
		b.WriteString(fmt.Sprintf(" COMMENT = '%s'", utils.EscapeString(*o.Comment)))
	}
	return b.String()
}

// Create a new schema with create options.
func (s *schemas) Create(ctx context.Context, o CreateOptions) (*Schema, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate create options: %w", err)
	}
	if _, err := s.client.Exec(ctx, o.build()); err != nil {
		return nil, fmt.Errorf("db exec: %w", err)
	}
	return s.Read(ctx, ReadOptions{Name: o.Name, DatabaseName: o.Database})
}
