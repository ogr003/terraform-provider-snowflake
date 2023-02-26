package tag

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ListOptions represents the options for listing tags.
type ListOptions struct {
	Database string
	Schema   string
}

func (o ListOptions) validate() error {
	if o.Database == "" {
		return errors.New("database name must not be empty")
	}
	if o.Schema == "" {
		return errors.New("schema name must not be empty")
	}
	return nil
}

// List all the users by list options.
func (t *tags) List(ctx context.Context, o ListOptions) ([]*Tag, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate list options: %w", err)
	}

	stmt := fmt.Sprintf(`SHOW %s IN SCHEMA "%s"."%s"`, ResourceTags, o.Database, o.Schema)
	rows, err := t.client.Query(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("do query: %w", err)
	}
	defer rows.Close()

	entities := []*Tag{}
	for rows.Next() {
		var entity tagEntity
		if err := rows.StructScan(&entity); err != nil {
			return nil, fmt.Errorf("rows scan: %w", err)
		}
		entities = append(entities, entity.toTag())
	}
	return entities, nil
}

// Read a tag by read options.
func (t *tags) Read(ctx context.Context, o Options) (*Tag, error) {
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("validate read options: %w", err)
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf(`SHOW %s LIKE "%s"`, ResourceTags, o.Name))
	if o.Database != "" {
		if o.Schema != "" {
			b.WriteString(fmt.Sprintf(` IN SCHEMA "%s"."%s"`, o.Database, o.Schema))
		} else {
			b.WriteString(fmt.Sprintf(` IN DATABASE "%s"`, o.Database))
		}
	}
	var entity tagEntity
	if err := t.client.Read(ctx, b.String(), &entity); err != nil {
		return nil, fmt.Errorf("read tag: %w", err)
	}
	return entity.toTag(), nil
}
