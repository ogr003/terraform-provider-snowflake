package user

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

type UpdateOptions struct {
	LoginName             *string
	DisplayName           *string
	FirstName             *string
	LastName              *string
	Email                 *string
	Disabled              *bool
	HasRSAPublicKey       *bool
	DefaultWarehouse      *string
	DefaultNamespace      *string
	DefaultRole           *string
	DefaultSecondaryRoles *[]string
	Comment               *string
}

func (o UpdateOptions) build() string {
	var b strings.Builder
	if o.LoginName != nil {
		b.WriteString(fmt.Sprintf(` LOGIN_NAME = '%s'`, *o.LoginName))
	}
	if o.DisplayName != nil {
		b.WriteString(fmt.Sprintf(` DISPLAY_NAME = '%s'`, *o.DisplayName))
	}
	if o.FirstName != nil {
		b.WriteString(fmt.Sprintf(` FIRST_NAME = '%s'`, *o.FirstName))
	}
	if o.LastName != nil {
		b.WriteString(fmt.Sprintf(` LAST_NAME = '%s'`, *o.LastName))
	}
	if o.Email != nil {
		b.WriteString(fmt.Sprintf(` EMAIL = '%s'`, *o.Email))
	}
	if o.Disabled != nil {
		b.WriteString(fmt.Sprintf(` DISABLED = %t`, *o.Disabled))
	}
	if o.DefaultWarehouse != nil {
		b.WriteString(fmt.Sprintf(` DEFAULT_WAREHOUSE = '%s'`, *o.DefaultWarehouse))
	}
	if o.DefaultNamespace != nil {
		b.WriteString(fmt.Sprintf(` DEFAULT_NAMESPACE = '%s'`, *o.DefaultNamespace))
	}
	if o.DefaultRole != nil {
		b.WriteString(fmt.Sprintf(` DEFAULT_ROLE = '%s'`, *o.DefaultRole))
	}
	if o.DefaultSecondaryRoles != nil {
		roles := []string{}
		for _, role := range *o.DefaultSecondaryRoles {
			roles = append(roles, "'"+role+"'")
		}
		b.WriteString(fmt.Sprintf(` DEFAULT_SECONDARY_ROLES = (%s)`, strings.Join(roles, ",")))
	}
	if o.Comment != nil {
		b.WriteString(fmt.Sprintf(` COMMENT = '%s'`, *o.Comment))
	}
	return b.String()
}

func (u *users) Update(ctx context.Context, name string, o UpdateOptions) (*User, error) {
	if name == "" {
		return nil, errors.New("name must not be empty")
	}
	stmt := fmt.Sprintf(`ALTER %s "%s" SET %s`, ResourceUser, name, o.build())
	if _, err := u.client.Exec(ctx, stmt); err != nil {
		return nil, fmt.Errorf("db exec: %w", err)
	}
	return u.Read(ctx, name)
}
