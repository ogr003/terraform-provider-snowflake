package table

import (
	"context"
	"fmt"
	"strings"

	"github.com/snowflakedb/terraform-provider-snowflake/sdk/tag"
	"github.com/snowflakedb/terraform-provider-snowflake/sdk/utils"
)

type KeyValue struct {
	Key   string
	Value interface{}
}

type UpdateOptions struct {
	Options

	DataRetentionTime         *int32
	ChangeTracking            *bool
	Comment                   *string
	RemoveComment             *bool
	ClusterBy                 *string
	DropClusteringKey         *bool
	DropPrimaryKey            *bool
	ChangePrimaryKey          *PrimaryKey
	AddTag                    *tag.Value
	UnsetTag                  *tag.Value
	ChangeTag                 *tag.Value
	DropColumnDefault         *string
	DropColumnName            *string
	ChangeColumnType          *KeyValue
	ChangeColumnComment       *KeyValue
	ChangeColumnMaskingPolicy *KeyValue
	ChangeNullConstraint      *KeyValue
}

func (o UpdateOptions) build() string {
	var b strings.Builder
	if o.RemoveComment != nil && *o.RemoveComment {
		b.WriteString(" UNSET COMMENT")
	} else if o.DataRetentionTime != nil {
		b.WriteString(fmt.Sprintf(" SET DATA_RETENTION_TIME_IN_DAYS = %d", *o.DataRetentionTime))
	} else if o.ChangeTracking != nil {
		b.WriteString(fmt.Sprintf(" SET CHANGE_TRACKING = %t", *o.ChangeTracking))
	} else if o.DropColumnName != nil {
		b.WriteString(fmt.Sprintf(` DROP COLUMN "%s"`, *o.DropColumnName))
	} else if o.Comment != nil {
		b.WriteString(fmt.Sprintf(" SET COMMENT = '%s'", utils.EscapeString(*o.Comment)))
	} else if o.ClusterBy != nil {
		b.WriteString(fmt.Sprintf(" CLUSTER BY LINEAR(%s)", *o.ClusterBy))
	} else if o.DropClusteringKey != nil && *o.DropClusteringKey {
		b.WriteString(" DROP CLUSTERING KEY")
	} else if o.DropPrimaryKey != nil && *o.DropPrimaryKey {
		b.WriteString(" DROP PRIMARY KEY")
	} else if o.ChangePrimaryKey != nil {
		key := strings.Join(utils.QuoteStrings(o.ChangePrimaryKey.Keys), ", ")
		if o.ChangePrimaryKey.Name != "" {
			b.WriteString(fmt.Sprintf(` ADD CONSTRAINT "%s" PRIMARY KEY(%s)`, o.ChangePrimaryKey.Name, key))
		} else {
			b.WriteString(fmt.Sprintf(` ADD PRIMARY KEY(%s)`, key))
		}
	} else if o.AddTag != nil {
		b.WriteString(fmt.Sprintf(` SET TAG "%s"."%s"."%s" = "%s"`, o.AddTag.Database, o.AddTag.Schema, o.AddTag.Name, o.AddTag.Value))
	} else if o.UnsetTag != nil {
		b.WriteString(fmt.Sprintf(` UNSET TAG "%s"."%s"."%s"`, o.UnsetTag.Database, o.UnsetTag.Schema, o.UnsetTag.Name))
	} else if o.ChangeTag != nil {
		b.WriteString(fmt.Sprintf(` SET TAG "%s"."%s"."%s" = "%s"`, o.ChangeTag.Database, o.ChangeTag.Schema, o.ChangeTag.Name, o.ChangeTag.Value))
	} else if o.DropColumnDefault != nil {
		b.WriteString(fmt.Sprintf(`MODIFY COLUMN "%s" DROP DEFAULT`, utils.EscapeString(*o.DropColumnDefault)))
	} else if o.ChangeColumnComment != nil {
		comment := o.ChangeColumnComment.Value.(string)
		b.WriteString(fmt.Sprintf(` MODIFY COLUMN "%s" COMMENT '%s'`, utils.EscapeString(o.ChangeColumnComment.Key), utils.EscapeString(comment)))
	} else if o.ChangeColumnType != nil {
		column := Column{
			Name: o.ChangeColumnType.Key,
			Type: o.ChangeColumnType.Value.(string),
		}
		b.WriteString(fmt.Sprintf(` MODIFY COLUMN %s`, column.ColumnDefinition(false, false)))
	} else if o.ChangeColumnMaskingPolicy != nil {
		maskingPolicy := strings.TrimSpace(o.ChangeColumnMaskingPolicy.Value.(string))
		if maskingPolicy == "" {
			b.WriteString(fmt.Sprintf(` MODIFY COLUMN "%s" UNSET MASKING POLICY`, utils.EscapeString(o.ChangeColumnMaskingPolicy.Key)))
		} else {
			b.WriteString(fmt.Sprintf(` MODIFY COLUMN "%s" SET MASKING POLICY %s`, utils.EscapeString(o.ChangeColumnMaskingPolicy.Key), utils.EscapeString(maskingPolicy)))
		}
	} else if o.ChangeNullConstraint != nil {
		nullable := o.ChangeNullConstraint.Value.(bool)
		if nullable {
			b.WriteString(fmt.Sprintf(` MODIFY COLUMN "%s" DROP NOT NULL`, o.ChangeNullConstraint.Key))
		} else {
			b.WriteString(fmt.Sprintf(` MODIFY COLUMN "%s" SET NOT NULL`, o.ChangeNullConstraint.Key))
		}
	}
	return b.String()
}

func (s *tables) Update(ctx context.Context, o UpdateOptions) (*Table, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate update options: %w", err)
	}
	stmt := fmt.Sprintf(`ALTER %s "%s" %s`, ResourceTable, QualifiedName(o.Name, o.Database, o.Schema), o.build())
	if _, err := s.client.Exec(ctx, stmt); err != nil {
		return nil, fmt.Errorf("db exec: %w", err)
	}
	return s.Read(ctx, Options{Name: o.Name, Database: o.Database})
}
