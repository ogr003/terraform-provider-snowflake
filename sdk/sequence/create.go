package sequence

import (
	"context"
	"fmt"
	"strings"
)

type CreateOptions struct {
	Options

	Start     *int
	Increment *int
	Comment   *string
}

func (o CreateOptions) validate() error {
	if err := o.Options.validate(); err != nil {
		return err
	}
	return nil
}

func (o CreateOptions) build() string {
	var b strings.Builder
	if o.Start != nil && *o.Start != 1 {
		b.WriteString(fmt.Sprintf(` START = %d`, *o.Start))
	}
	if o.Increment != nil && *o.Increment != 1 {
		b.WriteString(fmt.Sprintf(` INCREMENT = %d`, *o.Increment))
	}
	if o.Comment != nil && *o.Comment != "" {
		b.WriteString(fmt.Sprintf(` COMMENT = '%s'`, *o.Comment))
	}
	return b.String()
}

func (s *sequences) Create(ctx context.Context, options CreateOptions) (*Sequence, error) {
	if err := options.validate(); err != nil {
		return nil, fmt.Errorf("validate create options: %w", err)
	}
	stmt := fmt.Sprintf(`CREATE %s "%s" %s`, ResourceSequence, QualifiedName(options.Database, options.Schema, options.Name), options.build())
	if _, err := s.client.Exec(ctx, stmt); err != nil {
		return nil, fmt.Errorf("db exec: %w", err)
	}
	return s.Read(ctx, Options{Name: options.Name, Database: options.Database, Schema: options.Schema})
}
