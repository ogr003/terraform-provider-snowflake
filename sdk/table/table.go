package table

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/snowflakedb/terraform-provider-snowflake/sdk/client"
)

const (
	ResourceTable  = "TABLE"
	ResourceTables = "TABLES"
)

// Compile-time proof of interface implementation.
var _ Tables = (*tables)(nil)

// Tables describes all the tables related methods that the
// Snowflake API supports.
type Tables interface {
	List(ctx context.Context, o ListOptions) ([]*Table, error)
	Create(ctx context.Context, o CreateOptions) (*Table, error)
	Read(ctx context.Context, o Options) (*Table, error)
	Describe(ctx context.Context, o Options) ([]*DescribeTable, error)
	Update(ctx context.Context, o UpdateOptions) (*Table, error)
	Drop(ctx context.Context, o Options) error
	Rename(ctx context.Context, old string, new string) error
}

func New(c *client.Client) Tables {
	return &tables{
		client: c,
	}
}

type tables struct {
	client *client.Client
}

type Table struct {
	CreatedOn           string
	TableName           string
	DatabaseName        string
	SchemaName          string
	Kind                string
	Comment             string
	ClusterBy           string
	Rows                string
	Bytes               string
	Owner               string
	RetentionTime       int32
	AutomaticClustering string
	ChangeTracking      string
	IsExternal          string
}

type tableEntity struct {
	CreatedOn           sql.NullString `db:"created_on"`
	TableName           sql.NullString `db:"name"`
	DatabaseName        sql.NullString `db:"database_name"`
	SchemaName          sql.NullString `db:"schema_name"`
	Kind                sql.NullString `db:"kind"`
	Comment             sql.NullString `db:"comment"`
	ClusterBy           sql.NullString `db:"cluster_by"`
	Rows                sql.NullString `db:"row"`
	Bytes               sql.NullString `db:"bytes"`
	Owner               sql.NullString `db:"owner"`
	RetentionTime       sql.NullInt32  `db:"retention_time"`
	AutomaticClustering sql.NullString `db:"automatic_clustering"`
	ChangeTracking      sql.NullString `db:"change_tracking"`
	IsExternal          sql.NullString `db:"is_external"`
}

func (t *tableEntity) toTable() *Table {
	return &Table{
		CreatedOn:           t.CreatedOn.String,
		TableName:           t.TableName.String,
		DatabaseName:        t.DatabaseName.String,
		SchemaName:          t.SchemaName.String,
		Kind:                t.Kind.String,
		Comment:             t.Comment.String,
		ClusterBy:           t.ClusterBy.String,
		Rows:                t.Rows.String,
		Bytes:               t.Bytes.String,
		Owner:               t.Owner.String,
		RetentionTime:       t.RetentionTime.Int32,
		AutomaticClustering: t.AutomaticClustering.String,
		ChangeTracking:      t.ChangeTracking.String,
		IsExternal:          t.IsExternal.String,
	}
}

func QualifiedName(name string, db string, schema string) string {
	var s strings.Builder
	if db != "" && schema != "" {
		s.WriteString(fmt.Sprintf(`"%s"."%s".`, db, schema))
	}
	if db != "" && schema == "" {
		s.WriteString(fmt.Sprintf(`"%s"..`, db))
	}
	if db == "" && schema != "" {
		s.WriteString(fmt.Sprintf(`"%s".`, schema))
	}
	s.WriteString(fmt.Sprintf(`"%s"`, name))
	return s.String()
}

type Options struct {
	Name     string
	Database string
	Schema   string
}

func (o Options) validate() error {
	if o.Name == "" {
		return fmt.Errorf("table name is required")
	}
	if o.Database == "" {
		return fmt.Errorf("database is required")
	}
	if o.Schema == "" {
		return fmt.Errorf("schema is required")
	}
	return nil
}

func (s *tables) Drop(ctx context.Context, o Options) error {
	if err := o.validate(); err != nil {
		return fmt.Errorf("validate options: %w", err)
	}
	return s.client.Drop(ctx, ResourceTable, QualifiedName(o.Name, o.Database, o.Schema))
}

func (t *tables) Rename(ctx context.Context, old string, new string) error {
	return t.client.Rename(ctx, ResourceTable, old, new)
}
