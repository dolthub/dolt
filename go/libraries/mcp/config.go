package mcp

import (
	"fmt"
	"strings"
)

type Config struct {
	DSN             string `yaml:"dsn" json:"dsn"`
	Host            string `yaml:"host" json:"host"`
	User            string `yaml:"user" json:"user"`
	Password        string `yaml:"password" json:"password"`
	Branch          string `yaml:"branch" json:"branch"`
	DatabaseName    string `yaml:"database_name" json:"database_name"`
	Port            int    `yaml:"port" json:"port"`
	ParseTime       bool   `yaml:"parse_time" json:"parse_time"`
	MultiStatements bool   `yaml:"multi_statements" json:"multi_statements"`
}

func (c *Config) getDSNOptions() string {
	options := []string{}

	if c.ParseTime {
		options = append(options, "parseTime=true")
	}

	if c.MultiStatements {
		options = append(options, "multiStatements=true")
	}

	if len(options) > 0 {
		return strings.Join(options, "&")
	}

	return ""
}

func (c *Config) GetDSN() string {
	if c.DSN != "" {
		return c.DSN
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", c.User, c.Password, c.Host, c.Port, c.DatabaseName)

	if c.Branch != "" {
		dsn += fmt.Sprintf(".%s", c.Branch)
	}

	dsn += c.getDSNOptions()
	return dsn
}

