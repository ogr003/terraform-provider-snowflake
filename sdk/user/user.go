package user

import (
	"context"
	"database/sql"
	"strings"

	"github.com/snowflakedb/terraform-provider-snowflake/sdk/client"
)

const (
	ResourceUser  = "USER"
	ResourceUsers = "USERS"
)

// Compile-time proof of interface implementation.
var _ Users = (*users)(nil)

// Users describes all the users related methods that the
// Snowflake API supports.
type Users interface {
	List(ctx context.Context, options ListOptions) ([]*User, error)
	Create(ctx context.Context, options CreateOptions) (*User, error)
	Read(ctx context.Context, name string) (*User, error)
	Describe(ctx context.Context, name string) (*User, error)
	Update(ctx context.Context, name string, options UpdateOptions) (*User, error)
	Drop(ctx context.Context, user string) error
	Rename(ctx context.Context, old string, new string) error
}

func New(c *client.Client) Users {
	return &users{
		client: c,
	}
}

type users struct {
	client *client.Client
}

type User struct {
	Comment               string
	DefaultNamespace      string
	DefaultRole           string
	DefaultSecondaryRoles []string
	DefaultWarehouse      string
	Disabled              bool
	DisplayName           string
	Email                 string
	FirstName             string
	HasRsaPublicKey       bool
	LastName              string
	LoginName             string
	Name                  string
}

type userEntity struct {
	Name                  sql.NullString `db:"name"`
	Comment               sql.NullString `db:"comment"`
	DefaultNamespace      sql.NullString `db:"default_namespace"`
	DefaultRole           sql.NullString `db:"default_role"`
	DefaultSecondaryRoles sql.NullString `db:"default_secondary_roles"`
	DefaultWarehouse      sql.NullString `db:"default_warehouse"`
	Disabled              sql.NullBool   `db:"disabled"`
	DisplayName           sql.NullString `db:"display_name"`
	Email                 sql.NullString `db:"email"`
	FirstName             sql.NullString `db:"first_name"`
	HasRsaPublicKey       sql.NullBool   `db:"has_rsa_public_key"`
	LastName              sql.NullString `db:"last_name"`
	LoginName             sql.NullString `db:"login_name"`
}

type describeUserEntity struct {
	Property string         `db:"property"`
	Value    sql.NullString `db:"value"`
}

func (e *userEntity) toUser() *User {
	var roles []string
	if e.DefaultSecondaryRoles.Valid {
		value := strings.Trim(e.DefaultSecondaryRoles.String, "[]")
		roles = strings.Split(value, ",")
	}
	return &User{
		Comment:               e.Comment.String,
		DefaultNamespace:      e.DefaultNamespace.String,
		DefaultRole:           e.DefaultRole.String,
		DefaultSecondaryRoles: roles,
		DefaultWarehouse:      e.DefaultWarehouse.String,
		Disabled:              e.Disabled.Bool,
		DisplayName:           e.DisplayName.String,
		Email:                 e.Email.String,
		FirstName:             e.FirstName.String,
		HasRsaPublicKey:       e.HasRsaPublicKey.Bool,
		LastName:              e.LastName.String,
		LoginName:             e.LoginName.String,
		Name:                  e.Name.String,
	}
}

func (u *users) Drop(ctx context.Context, user string) error {
	return u.client.Drop(ctx, ResourceUser, user)
}

func (u *users) Rename(ctx context.Context, old string, new string) error {
	return u.client.Rename(ctx, ResourceUser, old, new)
}
