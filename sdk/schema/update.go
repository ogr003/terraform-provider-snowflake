package schema

import (
	"context"
	"errors"
	"fmt"
)

type UpdateOptions struct {
	DataRetentionTime *int32
	Comment           *string
}

func (o UpdateOptions) build() string {
	return ""
}

func (s *schemas) Update(ctx context.Context, name string, options UpdateOptions) (*Schema, error) {
	if name == "" {
		return nil, errors.New("name must not be empty")
	}
	stmt := fmt.Sprintf(`ALTER %s "%s" SET %s`, ResourceSchema, name, options.build())
	if _, err := s.client.Exec(ctx, stmt); err != nil {
		return nil, fmt.Errorf("db exec: %w", err)
	}
	return s.Read(ctx, ReadOptions{Name: name})
}
