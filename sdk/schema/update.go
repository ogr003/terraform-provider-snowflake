package schema

import (
	"context"
	"fmt"
	"strings"

	"github.com/snowflakedb/terraform-provider-snowflake/sdk/tag"
	"github.com/snowflakedb/terraform-provider-snowflake/sdk/utils"
)

type UpdateOptions struct {
	Options

	DataRetentionTime       *int32
	RemoveDataRetentionTime *bool
	Comment                 *string
	RemoveComment           *bool
	ManagedAccess           *bool
	RemoveManagedAccess     *bool
	TargetSchema            *string
	AddTag                  *tag.Value
	UnsetTag                *tag.Value
	ChangeTag               *tag.Value
}

func (o UpdateOptions) build() string {
	var b strings.Builder
	if o.RemoveComment != nil && *o.RemoveComment {
		b.WriteString(" UNSET COMMENT")
	} else if o.RemoveDataRetentionTime != nil && *o.RemoveDataRetentionTime {
		b.WriteString(" UNSET DATA_RETENTION_TIME_IN_DAYS")
	} else if o.RemoveManagedAccess != nil && *o.RemoveManagedAccess {
		b.WriteString(" DISABLE MANAGED ACCESS")
	} else if o.ManagedAccess != nil && *o.ManagedAccess {
		b.WriteString(" ENABLE MANAGED ACCESS")
	} else if o.DataRetentionTime != nil {
		b.WriteString(fmt.Sprintf(" SET DATA_RETENTION_TIME_IN_DAYS = %d", *o.DataRetentionTime))
	} else if o.Comment != nil {
		b.WriteString(fmt.Sprintf(" SET COMMENT = '%s'", utils.EscapeString(*o.Comment)))
	} else if o.TargetSchema != nil {
		b.WriteString(fmt.Sprintf(" SWAP WITH %s", *o.TargetSchema))
	} else if o.AddTag != nil {
		b.WriteString(fmt.Sprintf(` SET TAG "%s"."%s"."%s" = "%s"`, o.AddTag.Database, o.AddTag.Schema, o.AddTag.Name, o.AddTag.Value))
	} else if o.UnsetTag != nil {
		b.WriteString(fmt.Sprintf(` UNSET TAG "%s"."%s"."%s"`, o.UnsetTag.Database, o.UnsetTag.Schema, o.UnsetTag.Name))
	} else if o.ChangeTag != nil {
		b.WriteString(fmt.Sprintf(` SET TAG "%s"."%s"."%s" = "%s"`, o.ChangeTag.Database, o.ChangeTag.Schema, o.ChangeTag.Name, o.ChangeTag.Value))
	}
	return b.String()
}

func (s *schemas) Update(ctx context.Context, o UpdateOptions) (*Schema, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate update options: %w", err)
	}
	stmt := fmt.Sprintf(`ALTER %s "%s" %s`, ResourceSchema, QualifiedName(o.Name, o.Database), o.build())
	if _, err := s.client.Exec(ctx, stmt); err != nil {
		return nil, fmt.Errorf("db exec: %w", err)
	}
	return s.Read(ctx, ReadOptions{Name: o.Name, Database: o.Database})
}
