package config

import (
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
	// Set default values for viper
	v.SetDefault("host", "127.0.0.1")
	v.SetDefault("port", "7788")

	defaultLogger := logger.DefaultOptions()
	v.SetDefault("logger", map[string]interface{}{
		"level":             defaultLogger.Level,
		"max-size":          defaultLogger.MaxSize,
		"max-backups":       defaultLogger.MaxBackups,
		"max-age":           defaultLogger.MaxAge,
		"compress":          defaultLogger.Compress,
		"development":       defaultLogger.Development,
		"enable-console":    defaultLogger.EnableConsole,
		"enable-caller":     defaultLogger.EnableCaller,
		"enable-stacktrace": defaultLogger.EnableStacktrace,
	})

	v.SetConfigName("config")   // Configuration file name (without extension)
	v.SetConfigType("toml")     // Configuration file type
	v.AddConfigPath(".")        // Configuration file path
	v.AddConfigPath("./config") // Configuration file path
	// Read configuration file
	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}
	// Read from environment variables (higher priority)
	v.AutomaticEnv()

	var config Config
	// Unmarshal configuration
	if err := v.Unmarshal(&config); err != nil {
		return nil, err
	}

	return &config, nil
}
