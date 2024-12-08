package config

import (
	"os"
	"strings"
	"testing"

	"github.com/rleungx/tso/logger"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name        string
		configData  string
		wantErr     bool
		checkConfig func(*testing.T, *Config)
	}{
		{
			name: "Basic valid configuration",
			configData: `
host = "127.0.0.1"
port = 7788
backend = "etcd"
`,
			wantErr: false,
			checkConfig: func(t *testing.T, cfg *Config) {
				if cfg.Host != "127.0.0.1" {
					t.Errorf("Expected Host to be 127.0.0.1, got %s", cfg.Host)
				}
				if cfg.Port != 7788 {
					t.Errorf("Expected Port to be 7788, got %d", cfg.Port)
				}
				if cfg.Backend != "etcd" {
					t.Errorf("Expected Backend to be etcd, got %s", cfg.Backend)
				}
			},
		},
		{
			name: "Complete configuration",
			configData: `
host = "127.0.0.1"
port = 8080
backend = "etcd"
backend-address = "127.0.0.1:2379"
cacert-file = "/path/to/ca.crt"
cert-file = "/path/to/cert.crt"
key-file = "/path/to/key.pem"
[logger]
level = "info"
max-size = 100
max-backups = 5
max-age = 30
compress = true
enable-console = true
`,
			wantErr: false,
			checkConfig: func(t *testing.T, cfg *Config) {
				if cfg.BackendAddress != "127.0.0.1:2379" {
					t.Errorf("Expected BackendAddress to be 127.0.0.1:2379, got %s", cfg.BackendAddress)
				}
				if cfg.CAFile != "/path/to/ca.crt" {
					t.Errorf("Expected CAFile to be /path/to/ca.crt, got %s", cfg.CAFile)
				}
				if cfg.Logger.Level != "info" {
					t.Errorf("Expected Logger.Level to be info, got %s", cfg.Logger.Level)
				}
			},
		},
		{
			name: "Invalid configuration - empty Host",
			configData: `
host = ""
port = 7788
backend = "etcd"
`,
			wantErr: true,
		},
		{
			name: "Invalid configuration - port is 0",
			configData: `
host = "127.0.0.1"
port = 0
backend = "etcd"
`,
			wantErr: true,
		},
		{
			name: "Invalid configuration - empty Backend",
			configData: `
host = "127.0.0.1"
port = 7788
backend = ""
`,
			wantErr: true,
		},
		{
			name: "Logger configuration default values",
			configData: `
host = "127.0.0.1"
port = 7788
backend = "etcd"
`,
			wantErr: false,
			checkConfig: func(t *testing.T, cfg *Config) {
				if cfg.Logger == nil {
					t.Error("Expected Logger to be not nil")
					return
				}
				defaultLogger := logger.DefaultOptions()
				if cfg.Logger.Level != defaultLogger.Level {
					t.Errorf("Expected Logger.Level to be %s, got %s", defaultLogger.Level, cfg.Logger.Level)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := viper.New()
			v.SetConfigType("toml")
			err := v.ReadConfig(strings.NewReader(tt.configData))
			if err != nil {
				t.Fatalf("Failed to read config: %v", err)
			}

			cfg, err := LoadConfig(v)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && tt.checkConfig != nil {
				tt.checkConfig(t, cfg)
			}
		})
	}
}

func TestLoadConfigWithNilViper(t *testing.T) {
	cfg, err := LoadConfig(nil)
	if err != nil {
		t.Errorf("LoadConfig(nil) expected success, got error: %v", err)
	}
	if cfg == nil {
		t.Error("LoadConfig(nil) returned nil config")
	}
}

func TestLoadConfigWithInvalidConfigFile(t *testing.T) {
	v := viper.New()
	v.Set("config", "invalid.yaml")
	_, err := LoadConfig(v)
	require.Error(t, err)
}

func TestLoadConfigWithMemBackend(t *testing.T) {
	v := viper.New()
	v.Set("config", "")
	v.Set("backend", "mem")
	v.Set("backend-address", "")
	cfg, err := LoadConfig(v)
	require.NoError(t, err)
	require.Equal(t, "mem", cfg.Backend)
}

func TestLoadConfigWithUnmarshalError(t *testing.T) {
	v := viper.New()
	v.Set("config", "")
	v.Set("port", "invalid_port")
	_, err := LoadConfig(v)
	require.Error(t, err)
}

func TestLoadConfigEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		setupViper  func() *viper.Viper
		wantErr     bool
		errContains string
	}{
		{
			name: "port zero",
			setupViper: func() *viper.Viper {
				v := viper.New()
				v.Set("config", "")
				v.Set("port", 0)
				return v
			},
			wantErr:     true,
			errContains: "port must be between 1 and 65535",
		},
		{
			name: "port one",
			setupViper: func() *viper.Viper {
				v := viper.New()
				v.Set("config", "")
				v.Set("port", 1)
				return v
			},
			wantErr: false,
		},
		{
			name: "port max",
			setupViper: func() *viper.Viper {
				v := viper.New()
				v.Set("config", "")
				v.Set("port", 65535)
				return v
			},
			wantErr: false,
		},
		{
			name: "missing key file",
			setupViper: func() *viper.Viper {
				v := viper.New()
				v.Set("config", "")
				v.Set("cacert-file", "/path/to/ca.crt")
				v.Set("cert-file", "/path/to/cert.crt")
				return v
			},
			wantErr:     true,
			errContains: "private key file must be provided when using TLS",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := LoadConfig(tt.setupViper())
			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					require.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, cfg)
			}
		})
	}
}

func TestLoadConfigWithConfigFile(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "config*.yaml")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	// Write test configuration
	configContent := `
host: "0.0.0.0"
port: 8080
backend: "etcd"
backend-address: "http://localhost:2379"
logger:
  level: "debug"
  max-size: 100
  max-backups: 5
  max-age: 30
  compress: true
  enable-console: true
`
	err = os.WriteFile(tmpfile.Name(), []byte(configContent), 0644)
	require.NoError(t, err)

	// Use configuration file for testing
	v := viper.New()
	v.Set("config", tmpfile.Name())

	cfg, err := LoadConfig(v)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	// Verify configuration values
	require.Equal(t, "0.0.0.0", cfg.Host)
	require.Equal(t, 8080, cfg.Port)
	require.Equal(t, "etcd", cfg.Backend)
	require.Equal(t, "http://localhost:2379", cfg.BackendAddress)

	// Verify logging configuration
	require.Equal(t, "debug", cfg.Logger.Level)
	require.Equal(t, 100, cfg.Logger.MaxSize)
	require.Equal(t, 5, cfg.Logger.MaxBackups)
	require.Equal(t, 30, cfg.Logger.MaxAge)
	require.True(t, cfg.Logger.Compress)
	require.True(t, cfg.Logger.EnableConsole)
}

func TestConfigPrecedence(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "config*.yaml")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	// Write base configuration
	configContent := `
host: "127.0.0.1"
port: 7788
backend: "etcd"
backend-address: "localhost:2379"
logger:
  level: "info"
`
	err = os.WriteFile(tmpfile.Name(), []byte(configContent), 0644)
	require.NoError(t, err)

	// Set command line parameters (higher priority)
	v := viper.New()
	v.Set("config", tmpfile.Name())
	v.Set("host", "0.0.0.0")       // Override host from config file
	v.Set("port", 8080)            // Override port from config file
	v.Set("logger.level", "debug") // Override logger.level from config file

	cfg, err := LoadConfig(v)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	// Verify command line parameters correctly override config file values
	require.Equal(t, "0.0.0.0", cfg.Host)
	require.Equal(t, 8080, cfg.Port)
	require.Equal(t, "debug", cfg.Logger.Level)

	// Verify non-overridden values remain unchanged
	require.Equal(t, "etcd", cfg.Backend)
	require.Equal(t, "localhost:2379", cfg.BackendAddress)
}
