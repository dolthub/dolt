package sqlserver

import (
	"fmt"
	"net"
)

// LogLevel defines the available levels of logging for the server.
type LogLevel string

const (
	LogLevel_Debug   LogLevel = "debug"
	LogLevel_Info    LogLevel = "info"
	LogLevel_Warning LogLevel = "warning"
	LogLevel_Error   LogLevel = "error"
	LogLevel_Fatal   LogLevel = "fatal"
)

// ServerConfig contains all of the configurable options for the MySQL-compatible server.
type ServerConfig struct {
	Host     string   // The domain that the server will run on. Accepts an IPv4 or IPv6 address, in addition to localhost.
	Port     int      // The port that the server will run on. The valid range is [1024, 65535].
	User     string   // The username that connecting clients must use.
	Password string   // The password that connecting clients must use.
	Timeout  int      // The read and write timeouts.
	ReadOnly bool     // Whether the server will only accept read statements or all statements.
	LogLevel LogLevel // Specifies the level of logging that the server will use.
}

// DefaultServerConfig creates a `*ServerConfig` that has all of the options set to their default values.
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "",
		Timeout:  30,
		ReadOnly: false,
		LogLevel: LogLevel_Info,
	}
}

// Validate returns an `error` if any field is not valid.
func (config *ServerConfig) Validate() error {
	if config.Host != "localhost" {
		ip := net.ParseIP(config.Host)
		if ip == nil {
			return fmt.Errorf("address is not a valid IP: %v", config.Host)
		}
	}
	if config.Port < 1024 || config.Port > 65535 {
		return fmt.Errorf("port is not in the range between 1024-65535: %v\n", config.Port)
	}
	if len(config.User) == 0 {
		return fmt.Errorf("user cannot be empty")
	}
	if config.Timeout < 0 {
		return fmt.Errorf("timeout cannot be less than 0: %v\n", config.Timeout)
	}
	if config.LogLevel.String() == "unknown" {
		return fmt.Errorf("loglevel is invalid: %v\n", string(config.LogLevel))
	}
	return nil
}

// WithHost updates the host and returns the called `*ServerConfig`, which is useful for chaining calls.
func (config *ServerConfig) WithHost(host string) *ServerConfig {
	config.Host = host
	return config
}

// WithPort updates the port and returns the called `*ServerConfig`, which is useful for chaining calls.
func (config *ServerConfig) WithPort(port int) *ServerConfig {
	config.Port = port
	return config
}

// WithUser updates the user and returns the called `*ServerConfig`, which is useful for chaining calls.
func (config *ServerConfig) WithUser(user string) *ServerConfig {
	config.User = user
	return config
}

// WithPassword updates the password and returns the called `*ServerConfig`, which is useful for chaining calls.
func (config *ServerConfig) WithPassword(password string) *ServerConfig {
	config.Password = password
	return config
}

// WithTimeout updates the timeout and returns the called `*ServerConfig`, which is useful for chaining calls.
func (config *ServerConfig) WithTimeout(timeout int) *ServerConfig {
	config.Timeout = timeout
	return config
}

// WithReadOnly updates the read only flag and returns the called `*ServerConfig`, which is useful for chaining calls.
func (config *ServerConfig) WithReadOnly(readonly bool) *ServerConfig {
	config.ReadOnly = readonly
	return config
}

// WithLogLevel updates the log level and returns the called `*ServerConfig`, which is useful for chaining calls.
func (config *ServerConfig) WithLogLevel(loglevel LogLevel) *ServerConfig {
	config.LogLevel = loglevel
	return config
}

// ConnectionString returns a Data Source Name (DSN) to be used by go clients for connecting to a running server.
func (config *ServerConfig) ConnectionString() string {
	return fmt.Sprintf("%v:%v@tcp(%v:%v)/dolt", config.User, config.Password, config.Host, config.Port)
}

// String implements `fmt.Stringer`.
func (config *ServerConfig) String() string {
	return fmt.Sprintf(`HP="%v:%v"|U="%v"|P="%v"|T="%v"|R="%v"|L="%v"`, config.Host, config.Port, config.User,
		config.Password, config.Timeout, config.ReadOnly, config.LogLevel)
}

// String returns the string representation of the log level.
func (level LogLevel) String() string {
	switch level {
	case LogLevel_Debug:
		fallthrough
	case LogLevel_Info:
		fallthrough
	case LogLevel_Warning:
		fallthrough
	case LogLevel_Error:
		fallthrough
	case LogLevel_Fatal:
		return string(level)
	default:
		return "unknown"
	}
}
