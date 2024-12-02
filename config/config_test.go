package config

import (
	"strings"
	"testing"

	"github.com/rleungx/tso/logger"
	"github.com/spf13/viper"
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
