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
	v.SetConfigFile(v.GetString("config"))

	// Set default values
	v.SetDefault("host", "127.0.0.1")
	v.SetDefault("port", 7788)
	v.SetDefault("backend", "etcd")
	v.SetDefault("backend-address", "http://127.0.0.1:2379")

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
	if config.Port <= 0 || config.Port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535")
	}
	if config.Backend == "" {
		return fmt.Errorf("backend cannot be empty")
	}
	if config.Backend != "mem" && config.BackendAddress == "" {
		return fmt.Errorf("backend-address cannot be empty unless backend is 'mem'")
	}

	// If any certificate files are provided, all must be provided
	certFilesProvided := config.CAFile != "" || config.CertFile != "" || config.KeyFile != ""
	if certFilesProvided {
		if config.CAFile == "" {
			return fmt.Errorf("CA certificate file must be provided when using TLS")
		}
		if config.CertFile == "" {
			return fmt.Errorf("certificate file must be provided when using TLS")
		}
		if config.KeyFile == "" {
			return fmt.Errorf("private key file must be provided when using TLS")
		}
	}

	return nil
}
