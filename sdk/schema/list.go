package schema

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

type ListOptions struct {
	Database string
}

func (o ListOptions) validate() error {
	if o.Database == "" {
		return errors.New("database name must not be empty")
	}
	return nil
}

func (s *schemas) List(ctx context.Context, o ListOptions) ([]*Schema, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate list options: %w", err)
	}

	sql := fmt.Sprintf(`SHOW %s IN DATABASE "%s"`, ResourceSchemas, o.Database)
	rows, err := s.client.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("do query: %w", err)
	}
	defer rows.Close()

	entities := []*Schema{}
	for rows.Next() {
		var entity schemaEntity
		if err := rows.StructScan(&entity); err != nil {
			return nil, fmt.Errorf("rows scan: %w", err)
		}
		entities = append(entities, entity.toSchema())
	}
	return entities, nil
}

type ReadOptions struct {
	Name     string
	Database string
}

func (o ReadOptions) validate() error {
	if o.Name == "" {
		return errors.New("name must not be empty")
	}
	return nil
}

func (s *schemas) Read(ctx context.Context, o ReadOptions) (*Schema, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate read options: %w", err)
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf(`SHOW %s LIKE "%s"`, ResourceSchemas, o.Name))
	if o.Database != "" {
		b.WriteString(fmt.Sprintf(` IN DATABASE "%s"`, o.Database))
	}
	var entity schemaEntity
	if err := s.client.Read(ctx, b.String(), &entity); err != nil {
		return nil, fmt.Errorf("read schema: %w", err)
	}
	return entity.toSchema(), nil
}
