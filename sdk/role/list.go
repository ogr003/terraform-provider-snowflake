package role

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

type ListOptions struct {
	Name string
}

func (o ListOptions) validate() error {
	if o.Name == "" {
		return errors.New("role name is required")
	}
	return nil
}

func (r *roles) List(ctx context.Context, o ListOptions) ([]*Role, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate list options: %w", err)
	}

	var b strings.Builder
	b.WriteString("SHOW ROLES")
	if o.Name != "" {
		b.WriteString(fmt.Sprintf(" LIKE '%s'", o.Name))
	}
	rows, err := r.client.Query(ctx, b.String())
	if err != nil {
		return nil, fmt.Errorf("do query: %w", err)
	}
	defer rows.Close()

	entities := []*Role{}
	for rows.Next() {
		var entity roleEntity
		if err := rows.StructScan(&entity); err != nil {
			return nil, fmt.Errorf("rows scan: %w", err)
		}
		entities = append(entities, entity.toRole())
	}
	return entities, nil
}

func (r *roles) Read(ctx context.Context, name string) (*Role, error) {
	stmt := fmt.Sprintf(`SHOW %s LIKE '%s'`, ResourceRoles, name)
	var entity roleEntity
	if err := r.client.Read(ctx, stmt, &entity); err != nil {
		return nil, err
	}
	return entity.toRole(), nil
}
