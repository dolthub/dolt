package mcp

import (
	"fmt"
	"strings"
)

type Config interface {
	GetDSN() string
}

type configImpl struct {
	DSN             string `yaml:"dsn" json:"dsn"`
	Host            string `yaml:"host" json:"host"`
	User            string `yaml:"user" json:"user"`
	Password        string `yaml:"password" json:"password"`
	Branch          string `yaml:"branch" json:"branch"`
	CommitSha       string `yaml:"commit_sha" json:"commit_sha"`
	DatabaseName    string `yaml:"database_name" json:"database_name"`
	Port            int    `yaml:"port" json:"port"`
	ParseTime       bool   `yaml:"parse_time" json:"parse_time"`
	MultiStatements bool   `yaml:"multi_statements" json:"multi_statements"`
}

var _ Config = &configImpl{}

func NewConfig() Config {
	return &configImpl{}
}

func (c *configImpl) WithDSN(dsn string) {
	c.DSN = dsn
}

func (c *configImpl) WithHost(host string) {
	c.Host = host
}

func (c *configImpl) WithUser(user string) {
	c.User = user
}

func (c *configImpl) WithPassword(password string) {
	c.Password = password
}

func (c *configImpl) WithBranch(branch string) {
	c.Branch = branch
}

func (c *configImpl) WithCommitSha(commitSha string) {
	c.CommitSha = commitSha
}

func (c *configImpl) WithDatabaseName(databaseName string) {
	c.DatabaseName = databaseName
}

func (c *configImpl) WithPort(port int) {
	c.Port = port
}

func (c *configImpl) WithParseTime(parseTime bool) {
	c.ParseTime = parseTime
}

func (c *configImpl) WithMultiStatements(multiStatements bool) {
	c.MultiStatements = multiStatements
}

func (c *configImpl) getDSNOptions() string {
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

func (c *configImpl) GetDSN() string {
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
