package view

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/snowflakedb/terraform-provider-snowflake/sdk/client"
)

const (
	ResourceView  = "VIEW"
	ResourceViews = "VIEWS"
)

// Compile-time proof of interface implementation.
var _ Views = (*views)(nil)

// Views describes all the views related methods that the
// Snowflake API supports.
type Views interface {
	List(ctx context.Context, o ListOptions) ([]*View, error)
	Create(ctx context.Context, o CreateOptions) (*View, error)
	Read(ctx context.Context, o Options) (*View, error)
	Update(ctx context.Context, o UpdateOptions) (*View, error)
	Drop(ctx context.Context, o Options) error
	Rename(ctx context.Context, old string, name string) error
}

func New(c *client.Client) Views {
	return &views{
		client: c,
	}
}

type views struct {
	client *client.Client
}

type View struct {
	Comment      string
	IsSecure     bool
	Name         string
	SchemaName   string
	Text         string
	DatabaseName string
}

type viewEntity struct {
	Comment      sql.NullString `db:"comment"`
	IsSecure     sql.NullBool   `db:"is_secure"`
	Name         sql.NullString `db:"name"`
	SchemaName   sql.NullString `db:"schema_name"`
	Text         sql.NullString `db:"text"`
	DatabaseName sql.NullString `db:"database_name"`
}

func (v *viewEntity) toView() *View {
	return &View{
		Comment:      v.Comment.String,
		IsSecure:     v.IsSecure.Bool,
		Name:         v.Name.String,
		SchemaName:   v.SchemaName.String,
		Text:         v.Text.String,
		DatabaseName: v.DatabaseName.String,
	}
}

func QualifiedName(database, schema, name string) string {
	return fmt.Sprintf(`"%v"."%v"."%v"`, database, schema, name)
}

type Options struct {
	Name     string
	Database string
	Schema   string
}

func (o Options) validate() error {
	if o.Name == "" {
		return fmt.Errorf("view name is required")
	}
	if o.Database == "" {
		return fmt.Errorf("database name is required")
	}
	if o.Schema == "" {
		return fmt.Errorf("schema name is required")
	}
	return nil
}

func (v *views) Drop(ctx context.Context, o Options) error {
	if err := o.validate(); err != nil {
		return fmt.Errorf("validate drop options: %w", err)
	}
	return v.client.Drop(ctx, ResourceView, QualifiedName(o.Database, o.Schema, o.Name))
}

func (v *views) Rename(ctx context.Context, old, new string) error {
	return v.client.Rename(ctx, ResourceView, old, new)
}
