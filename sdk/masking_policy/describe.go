package masking_policy

import (
	"context"
	"database/sql"
	"fmt"
)

type DescribeMaskingPolicy struct {
	Name       string
	Signature  string
	ReturnType string
	Body       string
}

type describeMaskingPolicyEntity struct {
	Name       sql.NullString `db:"name"`
	Signature  sql.NullString `db:"signature"`
	ReturnType sql.NullString `db:"return_type"`
	Body       sql.NullString `db:"body"`
}

func (d *describeMaskingPolicyEntity) toDescribeMaskingPolicy() *DescribeMaskingPolicy {
	return &DescribeMaskingPolicy{
		Name:       d.Name.String,
		Signature:  d.Signature.String,
		ReturnType: d.ReturnType.String,
		Body:       d.Body.String,
	}
}

func (m *maskingPolicies) Describe(ctx context.Context, o Options) (*DescribeMaskingPolicy, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate describe options: %w", err)
	}
	stmt := fmt.Sprintf(`DESCRIBE %s LIKE '%s'`, ResourceMaskingPolicy, QualifiedName(o.Name, o.Database, o.Schema))
	var entity describeMaskingPolicyEntity
	if err := m.client.Describe(ctx, stmt, &entity); err != nil {
		return nil, fmt.Errorf("do describe: %w", err)
	}
	return entity.toDescribeMaskingPolicy(), nil
}
