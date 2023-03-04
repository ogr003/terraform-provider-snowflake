package role

import (
	"context"
	"database/sql"

	"github.com/snowflakedb/terraform-provider-snowflake/sdk/client"
)

const (
	ResourceRole  = "ROLE"
	ResourceRoles = "ROLES"
)

// Roles describes all the roles related methods that the
// Snowflake API supports.
type Roles interface {
	List(ctx context.Context, o ListOptions) ([]*Role, error)
	Read(ctx context.Context, name string) (*Role, error)
}

func New(c *client.Client) Roles {
	return &roles{
		client: c,
	}
}

type roles struct {
	client *client.Client
}

type Role struct {
	Name    string
	Comment string
	Owner   string
}

type roleEntity struct {
	Name    sql.NullString `db:"name"`
	Comment sql.NullString `db:"comment"`
	Owner   sql.NullString `db:"owner"`
}

func (r *roleEntity) toRole() *Role {
	return &Role{
		Name:    r.Name.String,
		Comment: r.Comment.String,
		Owner:   r.Owner.String,
	}
}
