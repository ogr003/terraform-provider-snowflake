package sequence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/snowflakedb/terraform-provider-snowflake/sdk/client"
)

const (
	ResourceSequence  = "SEQUENCE"
	ResourceSequences = "SEQUENCES"
)

// Compile-time proof of interface implementation.
var _ Sequences = (*sequences)(nil)

// Sequences describes all the sequences related methods that the
// Snowflake API supports.
type Sequences interface {
	List(ctx context.Context, o ListOptions) ([]*Sequence, error)
	Create(ctx context.Context, o CreateOptions) (*Sequence, error)
	Read(ctx context.Context, o Options) (*Sequence, error)
	Drop(ctx context.Context, o Options) error
}

func New(c *client.Client) Sequences {
	return &sequences{
		client: c,
	}
}

type sequences struct {
	client *client.Client
}

type Sequence struct {
	Name      string
	Database  string
	Schema    string
	NextValue string
	Increment string
	CreatedOn string
	Owner     string
	Comment   string
}

type sequenceEntity struct {
	Name      sql.NullString `db:"name"`
	Database  sql.NullString `db:"database_name"`
	Schema    sql.NullString `db:"schema_name"`
	NextValue sql.NullString `db:"next_value"`
	Increment sql.NullString `db:"interval"`
	CreatedOn sql.NullString `db:"created_on"`
	Owner     sql.NullString `db:"owner"`
	Comment   sql.NullString `db:"comment"`
}

func (s *sequenceEntity) toSequence() *Sequence {
	return &Sequence{
		Name:      s.Name.String,
		Database:  s.Database.String,
		Schema:    s.Schema.String,
		NextValue: s.NextValue.String,
		Increment: s.Increment.String,
		CreatedOn: s.CreatedOn.String,
		Owner:     s.Owner.String,
		Comment:   s.Comment.String,
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
		return fmt.Errorf("sequence name is required")
	}
	if o.Database == "" {
		return fmt.Errorf("database name is required")
	}
	if o.Schema == "" {
		return fmt.Errorf("schema name is required")
	}
	return nil
}

func (s *sequences) Drop(ctx context.Context, o Options) error {
	return s.client.Drop(ctx, ResourceSequence, QualifiedName(o.Database, o.Schema, o.Name))
}
