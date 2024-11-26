package config

import (
	"fmt"

	"github.com/rleungx/tso/logger"
	"github.com/spf13/viper"
)

type Config struct {
	Host           string          `mapstructure:"host"`
	Port           int             `mapstructure:"port"`
	Backend        string          `mapstructure:"backend"`
	BackendAddress string          `mapstructure:"backend-address"`
	CAFile         string          `mapstructure:"cacert-file"`
	CertFile       string          `mapstructure:"cert-file"`
	KeyFile        string          `mapstructure:"key-file"`
	Logger         *logger.Options `mapstructure:"logger"`
}

// LoadConfig loads configuration from file and command line
func LoadConfig(v *viper.Viper) (*Config, error) {
	if v == nil {
		v = viper.New()
	}

	// Set default values
	v.SetDefault("host", "127.0.0.1")
	v.SetDefault("port", 7788)
	v.SetDefault("backend", "etcd")

	defaultLogger := logger.DefaultOptions()
	v.SetDefault("logger", map[string]interface{}{
		"level":          defaultLogger.Level,
		"max-size":       defaultLogger.MaxSize,
		"max-backups":    defaultLogger.MaxBackups,
		"max-age":        defaultLogger.MaxAge,
		"compress":       defaultLogger.Compress,
		"enable-console": defaultLogger.EnableConsole,
	})

	// Read configuration file
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, err
		}
	}

	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, err
	}

	// Validate configuration
	if err := validateConfig(&config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %v", err)
	}

	return &config, nil
}

// validateConfig validates the configuration
func validateConfig(config *Config) error {
	if config.Host == "" {
		return fmt.Errorf("host cannot be empty")
	}
	if config.Port <= 0 {
		return fmt.Errorf("port must be greater than 0")
	}
	if config.Backend == "" {
		return fmt.Errorf("backend cannot be empty")
	}
	return nil
}
