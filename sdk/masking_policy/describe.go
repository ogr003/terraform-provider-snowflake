package masking_policy

import (
	"context"
	"fmt"
)

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
