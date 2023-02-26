package user

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/snowflakedb/terraform-provider-snowflake/sdk/utils"
)

// Describe an user by its name.
func (u *users) Describe(ctx context.Context, name string) (*User, error) {
	stmt := fmt.Sprintf(`DESCRIBE %s LIKE '%s'`, ResourceUser, name)
	rows, err := u.client.Query(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("do query: %w", err)
	}
	defer rows.Close()

	var entity userEntity
	for rows.Next() {
		var describeEntity describeUserEntity
		if err := rows.StructScan(&describeEntity); err != nil {
			return nil, fmt.Errorf("rows scan: %w", err)
		}
		if describeEntity.Value.String == "null" {
			describeEntity.Value.Valid = false
			describeEntity.Value.String = ""
		}
		switch describeEntity.Property {
		case "COMMENT":
			entity.Comment = describeEntity.Value
		case "DEFAULT_NAMESPACE":
			entity.DefaultNamespace = describeEntity.Value
		case "DEFAULT_ROLE":
			entity.DefaultRole = describeEntity.Value
		case "DEFAULT_SECONDARY_ROLES":
			if len(describeEntity.Value.String) > 0 {
				defaultSecondaryRoles := utils.ListContentToString(describeEntity.Value.String)
				entity.DefaultSecondaryRoles = sql.NullString{Valid: true, String: defaultSecondaryRoles}
			} else {
				entity.DefaultSecondaryRoles = sql.NullString{Valid: false}
			}
		case "DEFAULT_WAREHOUSE":
			entity.DefaultWarehouse = describeEntity.Value
		case "DISPLAY_NAME":
			entity.DisplayName = describeEntity.Value
		case "EMAIL":
			entity.Email = describeEntity.Value
		case "FIRST_NAME":
			entity.FirstName = describeEntity.Value
		case "RSA_PUBLIC_KEY_FP":
			if describeEntity.Value.Valid {
				entity.HasRsaPublicKey = sql.NullBool{Valid: true, Bool: true}
			} else {
				entity.HasRsaPublicKey = sql.NullBool{Valid: true, Bool: false}
			}
		case "LAST_NAME":
			entity.LastName = describeEntity.Value
		case "LOGIN_NAME":
			entity.LoginName = describeEntity.Value
		case "NAME":
			entity.Name = describeEntity.Value
		}
	}
	return entity.toUser(), nil
}
