package table

import (
	"context"
	"fmt"
	"strings"

	"github.com/snowflakedb/terraform-provider-snowflake/sdk/tag"
	"github.com/snowflakedb/terraform-provider-snowflake/sdk/utils"
)

type CreateOptions struct {
	Options

	Comment           *string
	ClusterBy         *[]string
	DataRetentionTime int32
	ChangeTracking    bool
	Columns           []Column
	Tags              *[]tag.Value
}

func (o CreateOptions) tagsValue() string {
	var s strings.Builder
	for _, tag := range *o.Tags {
		if tag.Schema != "" {
			if tag.Database != "" {
				s.WriteString(fmt.Sprintf(`"%s".`, tag.Database))
			}
			s.WriteString(fmt.Sprintf(`"%s".`, tag.Schema))
		}
		s.WriteString(fmt.Sprintf(`"%s" = '%s', `, tag.Name, tag.Value))
	}
	return strings.TrimSuffix(s.String(), ", ")
}

func (o CreateOptions) build() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf(`CREATE %s "%s"`, ResourceTable, QualifiedName(o.Name, o.Database, o.Schema)))
	if o.Comment != nil && *o.Comment != "" {
		b.WriteString(fmt.Sprintf(` COMMENT = '%s'`, utils.EscapeString(*o.Comment)))
	}
	if o.ClusterBy != nil {
		b.WriteString(fmt.Sprintf(` CLUSTER BY LINEAR(%s)`, strings.Join(*o.ClusterBy, ", ")))
	}
	b.WriteString(fmt.Sprintf(` DATA_RETENTION_TIME_IN_DAYS = %d`, o.DataRetentionTime))
	b.WriteString(fmt.Sprintf(` CHANGE_TRACKING = %t`, o.ChangeTracking))
	if o.Tags != nil {
		b.WriteString(fmt.Sprintf(` WITH TAGS (%s)`, o.tagsValue()))
	}
	return b.String()
}

func (t *tables) Create(ctx context.Context, o CreateOptions) (*Table, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate create options: %w", err)
	}
	if _, err := t.client.Exec(ctx, o.build()); err != nil {
		return nil, fmt.Errorf("db exec: %w", err)
	}
	return t.Read(ctx, Options{Name: o.Name, Schema: o.Schema, Database: o.Database})
}
