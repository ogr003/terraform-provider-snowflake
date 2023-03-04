package table

import (
	"context"
	"database/sql"
	"fmt"
)

type DescribeTable struct {
	Name          string
	Type          string
	Kind          string
	Nullable      string
	Default       string
	Comment       string
	MaskingPolicy string
}

type describeTableEntity struct {
	Name          sql.NullString `db:"name"`
	Type          sql.NullString `db:"type"`
	Kind          sql.NullString `db:"kind"`
	Nullable      sql.NullString `db:"null?"`
	Default       sql.NullString `db:"default"`
	Comment       sql.NullString `db:"comment"`
	MaskingPolicy sql.NullString `db:"policy name"`
}

func (d *describeTableEntity) toDescribeTable() *DescribeTable {
	return &DescribeTable{
		Name:          d.Name.String,
		Type:          d.Type.String,
		Kind:          d.Kind.String,
		Nullable:      d.Nullable.String,
		Default:       d.Default.String,
		Comment:       d.Comment.String,
		MaskingPolicy: d.MaskingPolicy.String,
	}
}

func (t *tables) Describe(ctx context.Context, o Options) ([]*DescribeTable, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate describe options: %w", err)
	}
	stmt := fmt.Sprintf(`DESCRIBE %s '%s'`, ResourceTable, QualifiedName(o.Name, o.Database, o.Schema))
	rows, err := t.client.Query(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("do query: %w", err)
	}
	defer rows.Close()

	entities := []*DescribeTable{}
	for rows.Next() {
		var entity describeTableEntity
		if err := rows.StructScan(&entity); err != nil {
			return nil, fmt.Errorf("rows scan: %w", err)
		}
		entities = append(entities, entity.toDescribeTable())
	}
	return entities, nil
}
