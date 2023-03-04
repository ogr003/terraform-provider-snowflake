package user

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

type CreateOptions struct {
	Name                  string
	Comment               *string
	LoginName             *string
	Password              *string
	Disabled              *bool
	DefaultNamespace      *string
	DefaultRole           *string
	DefaultSecondaryRoles *[]string
	DefaultWarehouse      *string
	RSAPublicKey          *string
	RSAPublicKey2         *string
	MustChangePassword    *bool
	Email                 *string
	DisplayName           *string
	FirstName             *string
	LastName              *string
}

func (o CreateOptions) validate() error {
	if o.Name == "" {
		return errors.New("user name is required")
	}
	return nil
}

func (o CreateOptions) build() string {
	var b strings.Builder
	if o.LoginName != nil {
		b.WriteString(fmt.Sprintf(` LOGIN_NAME = '%s'`, *o.LoginName))
	}
	if o.Password != nil {
		b.WriteString(fmt.Sprintf(` PASSWORD = '%s'`, *o.Password))
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
	if o.MustChangePassword != nil {
		b.WriteString(fmt.Sprintf(` MUST_CHANGE_PASSWORD = %t`, *o.MustChangePassword))
	}
	if o.Disabled != nil {
		b.WriteString(fmt.Sprintf(` DISABLED = %t`, *o.Disabled))
	}
	if o.DefaultWarehouse != nil {
		b.WriteString(fmt.Sprintf(` DEFAULT_WAREHOUSE = '%s'`, *o.DefaultWarehouse))
	}
	if o.RSAPublicKey != nil {
		b.WriteString(fmt.Sprintf(` RSA_PUBLIC_KEY = '%s'`, *o.RSAPublicKey))
	}
	if o.RSAPublicKey2 != nil {
		b.WriteString(fmt.Sprintf(` RSA_PUBLIC_KEY_2 = '%s'`, *o.RSAPublicKey2))
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

func (u *users) Create(ctx context.Context, options CreateOptions) (*User, error) {
	if err := options.validate(); err != nil {
		return nil, fmt.Errorf("validate create options: %w", err)
	}
	stmt := fmt.Sprintf(`CREATE %s "%s" %s`, ResourceUser, options.Name, options.build())
	if _, err := u.client.Exec(ctx, stmt); err != nil {
		return nil, fmt.Errorf("db exec: %w", err)
	}
	return u.Read(ctx, options.Name)
}
