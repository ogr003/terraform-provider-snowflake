package sequence

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

func (s *sequences) List(ctx context.Context, o ListOptions) ([]*Sequence, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate list options: %w", err)
	}

	sql := fmt.Sprintf(`SHOW %s IN SCHEMA "%s"."%s"`, ResourceSequences, o.Database, o.Schema)
	rows, err := s.client.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("do query: %w", err)
	}
	defer rows.Close()

	entities := []*Sequence{}
	for rows.Next() {
		var entity sequenceEntity
		if err := rows.StructScan(&entity); err != nil {
			return nil, fmt.Errorf("rows scan: %w", err)
		}
		entities = append(entities, entity.toSequence())
	}
	return entities, nil
}

func (s *sequences) Read(ctx context.Context, o Options) (*Sequence, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate read options: %w", err)
	}
	stmt := fmt.Sprintf(`SHOW %s LIKE "%s" IN SCHEMA "%s"."%s"`, ResourceSequences, o.Name, o.Database, o.Schema)
	var entity sequenceEntity
	if err := s.client.Read(ctx, stmt, &entity); err != nil {
		return nil, fmt.Errorf("read sequence: %w", err)
	}
	return entity.toSequence(), nil
}
