package user

import (
	"context"
	"errors"
	"fmt"
)

type ListOptions struct {
	Name string
}

func (o ListOptions) validate() error {
	if o.Name == "" {
		return errors.New("user name is required")
	}
	return nil
}

func (u *users) List(ctx context.Context, options ListOptions) ([]*User, error) {
	if err := options.validate(); err != nil {
		return nil, fmt.Errorf("validate list options: %w", err)
	}

	stmt := fmt.Sprintf(`SHOW %s LIKE '%s'`, ResourceUsers, options.Name)
	rows, err := u.client.Query(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("do query: %w", err)
	}
	defer rows.Close()

	entities := []*User{}
	for rows.Next() {
		var entity userEntity
		if err := rows.StructScan(&entity); err != nil {
			return nil, fmt.Errorf("rows scan: %w", err)
		}
		entities = append(entities, entity.toUser())
	}
	return entities, nil
}

func (u *users) Read(ctx context.Context, name string) (*User, error) {
	stmt := fmt.Sprintf(`SHOW %s LIKE '%s'`, ResourceUsers, name)
	var entity userEntity
	if err := u.client.Read(ctx, stmt, &entity); err != nil {
		return nil, err
	}
	return entity.toUser(), nil
}
