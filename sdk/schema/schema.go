package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/snowflakedb/terraform-provider-snowflake/sdk/client"
)

const (
	ResourceSchema  = "SCHEMA"
	ResourceSchemas = "SCHEMAS"
)

// Compile-time proof of interface implementation.
var _ Schemas = (*schemas)(nil)

// Schemas describes all the schemas related methods that the
// Snowflake API supports.
type Schemas interface {
	List(ctx context.Context, o ListOptions) ([]*Schema, error)
	Create(ctx context.Context, o CreateOptions) (*Schema, error)
	Read(ctx context.Context, o ReadOptions) (*Schema, error)
	Update(ctx context.Context, o UpdateOptions) (*Schema, error)
	Drop(ctx context.Context, o Options) error
	Undrop(ctx context.Context, o Options) error
	Use(ctx context.Context, o Options) error
	Rename(ctx context.Context, old string, new string) error
}

func New(c *client.Client) Schemas {
	return &schemas{
		client: c,
	}
}

type schemas struct {
	client *client.Client
}

type Schema struct {
	Name          string
	Database      string
	Comment       string
	Options       string
	RetentionTime string
}

type schemaEntity struct {
	Name          sql.NullString `db:"name"`
	Database      sql.NullString `db:"database_name"`
	Comment       sql.NullString `db:"comment"`
	Options       sql.NullString `db:"options"`
	RetentionTime sql.NullString `db:"retention_time"`
}

func (s *schemaEntity) toSchema() *Schema {
	return &Schema{
		Name:          s.Name.String,
		Database:      s.Database.String,
		Comment:       s.Comment.String,
		Options:       s.Options.String,
		RetentionTime: s.RetentionTime.String,
	}
}

func QualifiedName(name string, db string) string {
	var b strings.Builder
	if db != "" {
		b.WriteString(fmt.Sprintf(`"%s".`, db))
	}
	b.WriteString(fmt.Sprintf(`"%s"`, name))
	return b.String()
}

type Options struct {
	Name     string
	Database string
}

func (o Options) validate() error {
	if o.Name == "" {
		return fmt.Errorf("schema name is required")
	}
	return nil
}

func (s *schemas) Drop(ctx context.Context, o Options) error {
	if err := o.validate(); err != nil {
		return fmt.Errorf("validate options: %w", err)
	}
	return s.client.Drop(ctx, ResourceSchema, QualifiedName(o.Name, o.Database))
}

func (s *schemas) Undrop(ctx context.Context, o Options) error {
	if err := o.validate(); err != nil {
		return fmt.Errorf("validate options: %w", err)
	}
	return s.client.Undrop(ctx, ResourceSchema, QualifiedName(o.Name, o.Database))
}

func (s *schemas) Use(ctx context.Context, o Options) error {
	if err := o.validate(); err != nil {
		return fmt.Errorf("validate options: %w", err)
	}
	return s.client.Use(ctx, ResourceSchema, QualifiedName(o.Name, o.Database))
}

func (s *schemas) Rename(ctx context.Context, old string, new string) error {
	return s.client.Rename(ctx, ResourceSchema, old, new)
}
