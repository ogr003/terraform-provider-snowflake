package view

import (
	"context"
	"fmt"
)

type ListOptions struct {
	Database string
	Schema   string
}

func (o ListOptions) validate() error {
	if o.Database == "" {
		return fmt.Errorf("database name is required")
	}
	if o.Schema == "" {
		return fmt.Errorf("schema name is required")
	}
	return nil
}

func (v *views) List(ctx context.Context, o ListOptions) ([]*View, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate list options: %w", err)
	}

	stmt := fmt.Sprintf(`SHOW %s IN SCHEMA "%s"."%s"`, ResourceViews, o.Database, o.Schema)
	rows, err := v.client.Query(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("do query: %w", err)
	}
	defer rows.Close()

	entities := []*View{}
	for rows.Next() {
		var entity viewEntity
		if err := rows.StructScan(&entity); err != nil {
			return nil, fmt.Errorf("rows scan: %w", err)
		}
		entities = append(entities, entity.toView())
	}
	return entities, nil
}

func (v *views) Read(ctx context.Context, o Options) (*View, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate read options: %w", err)
	}
	stmt := fmt.Sprintf(`SHOW %s LIKE "%s" IN SCHEMA "%s"."%s"`, ResourceViews, o.Name, o.Database, o.Schema)
	var entity viewEntity
	if err := v.client.Read(ctx, stmt, &entity); err != nil {
		return nil, fmt.Errorf("read view: %w", err)
	}
	return entity.toView(), nil
}
