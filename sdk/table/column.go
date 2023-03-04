package table

import (
	"fmt"
	"strings"

	"github.com/snowflakedb/terraform-provider-snowflake/sdk/utils"
)

type ColumnDefaultType int

const (
	ColumnDefaultTypeConstant = iota
	ColumnDefaultTypeSequence
	ColumnDefaultTypeExpression
)

type ColumnIdentity struct {
	StartNum int
	StepNum  int
}

type ColumnDefault struct {
	Type       ColumnDefaultType
	Expression string
}

func (d *ColumnDefault) String(columnType string) string {
	columnType = strings.ToUpper(columnType)
	switch d.Type {
	case ColumnDefaultTypeConstant:
		if strings.Contains(columnType, "CHAR") || columnType == "STRING" || columnType == "TEXT" {
			return utils.EscapeSnowflakeString(d.Expression)
		}
	case ColumnDefaultTypeSequence:
		return fmt.Sprintf(`%s.NEXTVAL`, d.Expression)
	}
	return d.Expression
}

type Column struct {
	Name          string
	Type          string
	Nullable      bool
	Default       *ColumnDefault
	Identity      *ColumnIdentity
	Comment       string
	MaskingPolicy string
}

func (c *Column) ColumnDefinition(inlineConstraints bool, comment bool) string {
	var s strings.Builder
	s.WriteString(fmt.Sprintf(`"%s" %s`, utils.EscapeString(c.Name), utils.EscapeString(c.Type)))
	if inlineConstraints {
		if !c.Nullable {
			s.WriteString(" NOT NULL")
		}
	}
	if c.Default != nil {
		s.WriteString(fmt.Sprintf(` DEFAULT %s`, c.Default.String(c.Type)))
	}
	if c.Identity != nil {
		s.WriteString(fmt.Sprintf(` IDENTITY(%d, %d)`, c.Identity.StartNum, c.Identity.StepNum))
	}
	if strings.TrimSpace(c.MaskingPolicy) != "" {
		s.WriteString(fmt.Sprintf(` WITH MASKING POLICY %s`, utils.EscapeString(c.MaskingPolicy)))
	}
	if comment {
		s.WriteString(fmt.Sprintf(` COMMENT = '%s'`, utils.EscapeString(c.Comment)))
	}
	return s.String()
}

type Columns []Column

type PrimaryKey struct {
	Name string
	Keys []string
}
