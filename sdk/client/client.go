package client

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/jmoiron/sqlx"
	"github.com/luna-duclos/instrumentedsql"
	"github.com/snowflakedb/gosnowflake"
	"github.com/snowflakedb/terraform-provider-snowflake/sdk/utils"
)

type Config struct {
	Account   string
	User      string
	Password  string
	Region    string
	Role      string
	Host      string
	Warehouse string
}

func DefaultConfig() *Config {
	config := &Config{
		Account:   os.Getenv("SNOWFLAKE_ACCOUNT"),
		User:      os.Getenv("SNOWFLAKE_USER"),
		Password:  os.Getenv("SNOWFLAKE_PASSWORD"),
		Region:    os.Getenv("SNOWFLAKE_REGION"),
		Role:      os.Getenv("SNOWFLAKE_ROLE"),
		Host:      os.Getenv("SNOWFLAKE_HOST"),
		Warehouse: os.Getenv("SNOWFLAKE_WAREHOUSE"),
	}
	// us-west-2 is Snowflake's default region, but if you actually specify that it won't trigger the default code
	//  https://github.com/snowflakedb/gosnowflake/blob/52137ce8c32eaf93b0bd22fc5c7297beff339812/dsn.go#L61
	if config.Region == "us-west-2" {
		config.Region = ""
	}
	return config
}

type Client struct {
	conn *sql.DB
}

func NewClient(cfg *Config) (*Client, error) {
	config := DefaultConfig()
	if cfg != nil {
		if cfg.Account != "" {
			config.Account = cfg.Account
		}
		if cfg.User != "" {
			config.User = cfg.User
		}
		if cfg.Password != "" {
			config.Password = cfg.Password
		}
		// us-west-2 is Snowflake's default region, but if you actually specify that it won't trigger the default code
		//  https://github.com/snowflakedb/gosnowflake/blob/52137ce8c32eaf93b0bd22fc5c7297beff339812/dsn.go#L61
		if cfg.Region != "" && cfg.Region != "us-west-2" {
			config.Region = cfg.Region
		}
		if cfg.Role != "" {
			config.Role = cfg.Role
		}
		if cfg.Host != "" {
			config.Host = cfg.Host
			// if host is set trust it and do not use the region
			config.Region = ""
		}
		if cfg.Warehouse != "" {
			config.Warehouse = cfg.Warehouse
		}
	}

	dsn, err := gosnowflake.DSN(&gosnowflake.Config{
		Account:   config.Account,
		User:      config.User,
		Password:  config.Password,
		Region:    config.Region,
		Role:      config.Role,
		Warehouse: config.Warehouse,
	})
	if err != nil {
		return nil, fmt.Errorf("build dsn for snowflake connection: %w", err)
	}

	logger := instrumentedsql.LoggerFunc(func(ctx context.Context, fn string, kv ...interface{}) {
		switch fn {
		case "sql-conn-query", "sql-conn-exec":
			log.Printf("[DEBUG] %s: %v", fn, kv)
		default:
			return
		}
	})
	sql.Register("snowflake-instrumented", instrumentedsql.WrapDriver(&gosnowflake.SnowflakeDriver{}, instrumentedsql.WithLogger(logger)))
	conn, err := sql.Open("snowflake-instrumented", dsn)
	if err != nil {
		return nil, fmt.Errorf("open snowflake connection: %w", err)
	}
	client := &Client{
		conn: conn,
	}
	return client, nil
}

// Close the client
func (c *Client) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

// Exec a sql statement
func (c *Client) Exec(ctx context.Context, sql string) (sql.Result, error) {
	return c.conn.ExecContext(ctx, sql)
}

// Query the resources with a sql statement
func (c *Client) Query(ctx context.Context, sql string) (*sqlx.Rows, error) {
	return sqlx.NewDb(c.conn, "snowflake-instrumented").Unsafe().QueryxContext(ctx, sql)
}

// Drop a resource
func (c *Client) Drop(ctx context.Context, resource string, name string) error {
	stmt := fmt.Sprintf(`DROP %s "%s"`, resource, name)
	if _, err := c.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("db exec: %w", err)
	}
	return nil
}

// Undrop a resource
func (c *Client) Undrop(ctx context.Context, resource string, name string) error {
	stmt := fmt.Sprintf(`UNDROP %s "%s"`, resource, name)
	if _, err := c.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("db exec: %w", err)
	}
	return nil
}

// Rename a resource
func (c *Client) Rename(ctx context.Context, resource string, old string, new string) error {
	stmt := fmt.Sprintf(`ALTER %s "%s" RENAME TO "%s"`, resource, old, new)
	if _, err := c.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("db exec: %w", err)
	}
	return nil
}

func (c *Client) Read(ctx context.Context, stmt string, v interface{}) error {
	rows, err := c.Query(ctx, stmt)
	if err != nil {
		return fmt.Errorf("do query: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return utils.ErrNoRecord
	}
	if err := rows.StructScan(v); err != nil {
		return fmt.Errorf("rows scan: %w", err)
	}
	return nil
}

func (c *Client) Describe(ctx context.Context, stmt string, v interface{}) error {
	rows, err := c.Query(ctx, stmt)
	if err != nil {
		return fmt.Errorf("do query: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return utils.ErrNoRecord
	}
	if err := rows.StructScan(v); err != nil {
		return fmt.Errorf("rows scan: %w", err)
	}
	return nil
}
