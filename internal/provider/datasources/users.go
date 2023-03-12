package datasources

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/snowflakedb/terraform-provider-snowflake/sdk/client"
	"github.com/snowflakedb/terraform-provider-snowflake/sdk/user"
)

type Users struct {
	users user.Users
}

func NewUsers(client *client.Client) *Users {
	return &Users{
		users: user.New(client),
	}
}

func (u *Users) GetSchema(ctx context.Context) (tfsdk.Schema, diag.Diagnostics) {
	return tfsdk.Schema{
		Description: "Users Data Source for the Snowflake Provider",
		Attributes: map[string]tfsdk.Attribute{
			"pattern": {
				Description: "Users pattern for which to return metadata. Please refer to LIKE keyword from Snowflake documentation [doc](https://docs.snowflake.com/en/sql-reference/sql/show-users.html#parameters)",
				Type:        types.StringType,
				Required:    true,
			},
			"users": {
				Computed:    true,
				Description: "List of users matching the pattern",
				Attributes: tfsdk.ListNestedAttributes(map[string]tfsdk.Attribute{
					"name": {
						Type:     types.StringType,
						Computed: true,
					},
					"login_name": {
						Type:     types.StringType,
						Optional: true,
						Computed: true,
					},
					"comment": {
						Type:     types.StringType,
						Optional: true,
						Computed: true,
					},
					"disabled": {
						Type:     types.BoolType,
						Optional: true,
						Computed: true,
					},
					"default_warehouse": {
						Type:     types.StringType,
						Optional: true,
						Computed: true,
					},
					"default_namespace": {
						Type:     types.StringType,
						Optional: true,
						Computed: true,
					},
					"default_role": {
						Type:     types.StringType,
						Optional: true,
						Computed: true,
					},
					"default_secondary_roles": {
						Type: types.SetType{
							ElemType: types.StringType,
						},
						Optional: true,
						Computed: true,
					},
					"has_rsa_public_key": {
						Type:     types.BoolType,
						Computed: true,
					},
					"email": {
						Type:     types.StringType,
						Optional: true,
						Computed: true,
					},
					"display_name": {
						Type:     types.StringType,
						Computed: true,
						Optional: true,
					},
					"first_name": {
						Type:     types.StringType,
						Optional: true,
						Computed: true,
					},
					"last_name": {
						Type:     types.StringType,
						Optional: true,
						Computed: true,
					},
				}),
			},
		},
	}, nil
}

func (u *Users) NewDataSource(ctx context.Context, prov tfsdk.Provider) (tfsdk.DataSource, diag.Diagnostics) {
	return u, nil
}

type UserData struct {
	Pattern string        `tfsdk:"pattern"`
	Users   []*UserEntity `tfsdk:"users"`
}

type UserEntity struct {
	Name                  string   `tfsdk:"name"`
	LoginName             string   `tfsdk:"login_name"`
	Comment               string   `tfsdk:"comment"`
	Disabled              bool     `tfsdk:"disabled"`
	DefaultWarehouse      string   `tfsdk:"default_warehouse"`
	DefaultNamespace      string   `tfsdk:"default_namespace"`
	DefaultRole           string   `tfsdk:"default_role"`
	DefaultSecondaryRoles []string `tfsdk:"default_secondary_roles"`
	HasRsaPublicKey       bool     `tfsdk:"has_rsa_public_key"`
	Email                 string   `tfsdk:"email"`
	DisplayName           string   `tfsdk:"display_name"`
	FirstName             string   `tfsdk:"first_name"`
	LastName              string   `tfsdk:"last_name"`
}

func (u *Users) Read(ctx context.Context, req tfsdk.ReadDataSourceRequest, resp *tfsdk.ReadDataSourceResponse) {
	var data UserData
	if diags := req.Config.Get(ctx, &data); diags.HasError() {
		resp.Diagnostics = diags
		return
	}
	users, err := u.users.List(ctx, user.ListOptions{
		Name: data.Pattern,
	})
	if err != nil {
		resp.Diagnostics.AddError("list users: %s", err.Error())
		return
	}
	for _, u := range users {
		data.Users = append(data.Users, &UserEntity{
			Name:                  u.Name,
			LoginName:             u.LoginName,
			Comment:               u.Comment,
			Disabled:              u.Disabled,
			DefaultWarehouse:      u.DefaultWarehouse,
			DefaultNamespace:      u.DefaultNamespace,
			DefaultRole:           u.DefaultRole,
			DefaultSecondaryRoles: u.DefaultSecondaryRoles,
			HasRsaPublicKey:       u.HasRsaPublicKey,
			Email:                 u.Email,
			DisplayName:           u.DisplayName,
			FirstName:             u.FirstName,
			LastName:              u.LastName,
		})
	}
	resp.Diagnostics = resp.State.Set(ctx, &data)
}
