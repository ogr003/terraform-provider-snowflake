package masking_policy

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/snowflakedb/terraform-provider-snowflake/sdk/client"
)

const (
	ResourceMaskingPolicy   = "MASKING POLICY"
	ResourceMaskingPolicies = "MASKING POLICIES"
)

// Compile-time proof of interface implementation.
var _ MaskingPolicies = (*maskingPolicies)(nil)

// MaskingPolicies describes all the masking policies related methods that the
// Snowflake API supports.
type MaskingPolicies interface {
	List(ctx context.Context, options ListOptions) ([]*MaskingPolicy, error)
	Create(ctx context.Context, options CreateOptions) (*MaskingPolicy, error)
	Read(ctx context.Context, o Options) (*MaskingPolicy, error)
	Describe(ctx context.Context, o Options) (*DescribeMaskingPolicy, error)
	Update(ctx context.Context, o UpdateOptions) (*MaskingPolicy, error)
	Drop(ctx context.Context, o Options) error
}

func New(client *client.Client) MaskingPolicies {
	return &maskingPolicies{
		client: client,
	}
}

type maskingPolicies struct {
	client *client.Client
}

type MaskingPolicy struct {
	Name      string
	Database  string
	Schema    string
	Kind      string
	Owner     string
	Comment   string
	CreatedOn string
}

type maskingPolicyEntity struct {
	Name      sql.NullString `db:"name"`
	Database  sql.NullString `db:"database_name"`
	Schema    sql.NullString `db:"schema_name"`
	Kind      sql.NullString `db:"kind"`
	Owner     sql.NullString `db:"owner"`
	Comment   sql.NullString `db:"comment"`
	CreatedOn sql.NullString `db:"created_on"`
}

func (e *maskingPolicyEntity) toMaskingPolicy() *MaskingPolicy {
	return &MaskingPolicy{
		Name:      e.Name.String,
		Database:  e.Database.String,
		Schema:    e.Schema.String,
		Kind:      e.Kind.String,
		Owner:     e.Owner.String,
		Comment:   e.Comment.String,
		CreatedOn: e.CreatedOn.String,
	}
}

func QualifiedName(name, db, schema string) string {
	var b strings.Builder
	if db != "" && schema != "" {
		b.WriteString(fmt.Sprintf(`"%s"."%s".`, db, schema))
	}
	if db != "" && schema == "" {
		b.WriteString(fmt.Sprintf(`"%s"..`, db))
	}
	if db == "" && schema != "" {
		b.WriteString(fmt.Sprintf(`"%s".`, schema))
	}
	b.WriteString(fmt.Sprintf(`"%s"`, name))
	return b.String()
}

type Options struct {
	Name     string
	Database string
	Schema   string
}

func (o Options) validate() error {
	if o.Name == "" {
		return errors.New("masking policy name is required")
	}
	return nil
}

func (m *maskingPolicies) Drop(ctx context.Context, o Options) error {
	if err := o.validate(); err != nil {
		return fmt.Errorf("validate drop options: %w", err)
	}
	return m.client.Drop(ctx, ResourceMaskingPolicy, QualifiedName(o.Name, o.Database, o.Schema))
}
