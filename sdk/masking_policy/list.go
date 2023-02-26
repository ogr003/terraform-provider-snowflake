package masking_policy

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
		return errors.New("database name must not be empty")
	}
	if o.Schema == "" {
		return errors.New("schema name must not be empty")
	}
	return nil
}

func (m *maskingPolicies) List(ctx context.Context, o ListOptions) ([]*MaskingPolicy, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate list options: %w", err)
	}

	stmt := fmt.Sprintf(`SHOW %s IN SCHEMA "%s"."%s"`, ResourceMaskingPolicies, o.Database, o.Schema)
	rows, err := m.client.Query(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("do query: %w", err)
	}
	defer rows.Close()

	entities := []*MaskingPolicy{}
	for rows.Next() {
		var entity maskingPolicyEntity
		if err := rows.StructScan(&entity); err != nil {
			return nil, fmt.Errorf("rows scan: %w", err)
		}
		entities = append(entities, entity.toMaskingPolicy())
	}
	return entities, nil
}

func (m *maskingPolicies) Read(ctx context.Context, o Options) (*MaskingPolicy, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate read options: %w", err)
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf(`SHOW %s LIKE "%s"`, ResourceMaskingPolicies, o.Name))
	if o.Database != "" {
		if o.Schema != "" {
			b.WriteString(fmt.Sprintf(` IN SCHEMA "%s"."%s"`, o.Database, o.Schema))
		} else {
			b.WriteString(fmt.Sprintf(` IN DATABASE "%s"`, o.Database))
		}
	}
	var entity maskingPolicyEntity
	if err := m.client.Read(ctx, b.String(), &entity); err != nil {
		return nil, fmt.Errorf("read masking policy: %w", err)
	}
	return entity.toMaskingPolicy(), nil
}
