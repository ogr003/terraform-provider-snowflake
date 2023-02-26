package tag

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/snowflakedb/terraform-provider-snowflake/sdk/client"
)

const (
	ResourceTag  = "TAG"
	ResourceTags = "TAGS"
)

// Compile-time proof of interface implementation.
var _ Tags = (*tags)(nil)

// Tags describes all the tags related methods that the
// Snowflake API supports.
type Tags interface {
	List(ctx context.Context, options ListOptions) ([]*Tag, error)
	Create(ctx context.Context, options CreateOptions) (*Tag, error)
	Read(ctx context.Context, o Options) (*Tag, error)
	Update(ctx context.Context, o UpdateOptions) (*Tag, error)
	Drop(ctx context.Context, o Options) error
	Undrop(ctx context.Context, o Options) error
}

func New(client *client.Client) Tags {
	return &tags{
		client: client,
	}
}

type tags struct {
	client *client.Client
}

type Tag struct {
	Name          string
	Database      string
	Schema        string
	Comment       string
	AllowedValues string
}

type Value struct {
	Name     string
	Database string
	Schema   string
	Value    string
}

type PolicyReference struct {
	PolicyDB        sql.NullString `db:"policy_db"`
	PolicySchema    sql.NullString `db:"policy_schema"`
	PolicyName      sql.NullString `db:"policy_name"`
	PolicyKind      sql.NullString `db:"policy_kind"`
	RefDB           sql.NullString `db:"ref_database_name"`
	RefSchema       sql.NullString `db:"ref_schema_name"`
	RefEntity       sql.NullString `db:"ref_entity_name"`
	RefEntityDomain sql.NullString `db:"ref_entity_domain"`
}

type tagEntity struct {
	Name          sql.NullString `db:"name"`
	Database      sql.NullString `db:"database_name"`
	Schema        sql.NullString `db:"schema_name"`
	Comment       sql.NullString `db:"comment"`
	AllowedValues sql.NullString `db:"allowed_values"`
}

func (t *tagEntity) toTag() *Tag {
	return &Tag{
		Name:          t.Name.String,
		Database:      t.Database.String,
		Schema:        t.Schema.String,
		Comment:       t.Comment.String,
		AllowedValues: t.AllowedValues.String,
	}
}

func QualifiedName(name, db, schema string) string {
	var b strings.Builder
	if db != "" {
		b.WriteString(fmt.Sprintf(`"%s".`, db))
	}
	if schema != "" {
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
		return fmt.Errorf("name must not be empty")
	}
	return nil
}

func (t *tags) Drop(ctx context.Context, o Options) error {
	if err := o.validate(); err != nil {
		return fmt.Errorf("validate drop options: %w", err)
	}
	return t.client.Drop(ctx, ResourceTag, QualifiedName(o.Name, o.Database, o.Schema))
}

func (t *tags) Undrop(ctx context.Context, o Options) error {
	if err := o.validate(); err != nil {
		return fmt.Errorf("validate undrop options: %w", err)
	}
	return t.client.Undrop(ctx, ResourceTag, QualifiedName(o.Name, o.Database, o.Schema))
}
