package table

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

type ListOptions struct {
	Database string
	Schema   string
}

func (o ListOptions) validate() error {
	if o.Database == "" {
		return errors.New("database is required")
	}
	if o.Schema == "" {
		return errors.New("schema is required")
	}
	return nil
}

func (t *tables) List(ctx context.Context, o ListOptions) ([]*Table, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate list options: %w", err)
	}

	stmt := fmt.Sprintf(`SHOW %s IN SCHEMA "%s"."%s"`, ResourceTables, o.Database, o.Schema)
	rows, err := t.client.Query(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("do query: %w", err)
	}
	defer rows.Close()

	entities := []*Table{}
	for rows.Next() {
		var entity tableEntity
		if err := rows.StructScan(&entity); err != nil {
			return nil, fmt.Errorf("rows scan: %w", err)
		}
		entities = append(entities, entity.toTable())
	}
	return entities, nil
}

func (t *tables) Read(ctx context.Context, o Options) (*Table, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate read options: %w", err)
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf(`SHOW %s LIKE "%s" IN SCHEMA "%s"."%s"`, ResourceTables, o.Name, o.Database, o.Schema))
	var entity tableEntity
	if err := t.client.Read(ctx, b.String(), &entity); err != nil {
		return nil, fmt.Errorf("read table: %w", err)
	}
	return entity.toTable(), nil
}
