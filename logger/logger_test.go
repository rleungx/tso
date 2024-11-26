package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestLogger(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("rotation", func(t *testing.T) {
		logFile := filepath.Join(tmpDir, "rotation.log")
		opts := &Options{
			Level:         "info",
			Format:        "json",
			Filename:      logFile,
			MaxSize:       1,
			MaxBackups:    2,
			MaxAge:        1,
			Compress:      false,
			EnableConsole: false,
		}
		require.NoError(t, Init(opts))

		data := strings.Repeat("a", 512)
		for i := 0; i < 5000; i++ {
			Info("test", zap.String("data", data))
		}
		Close()

		files, err := os.ReadDir(tmpDir)
		require.NoError(t, err)

		count := 0
		for _, file := range files {
			if strings.HasPrefix(file.Name(), "rotation") {
				count++
			}
		}
		require.Greater(t, count, 1, "There should be multiple log files")
	})

	t.Run("compression", func(t *testing.T) {
		logFile := filepath.Join(tmpDir, "compress.log")
		opts := &Options{
			Level:         "info",
			Format:        "json",
			Filename:      logFile,
			MaxSize:       1,
			MaxBackups:    2,
			MaxAge:        1,
			Compress:      true,
			EnableConsole: false,
		}
		require.NoError(t, Init(opts))

		data := strings.Repeat("a", 512)
		for i := 0; i < 5000; i++ {
			Info("test", zap.String("data", data))
		}
		Close()

		files, err := os.ReadDir(tmpDir)
		require.NoError(t, err)

		for _, file := range files {
			if strings.Contains(file.Name(), "compress") && strings.HasSuffix(file.Name(), ".gz") {
				return
			}
		}
		t.Fatal("No compressed file found")
	})

	t.Run("log level", func(t *testing.T) {
		validLevels := []string{"INFO", "info", "DEBUG", "debug", "WARN", "warn", "ERROR", "error", ""}

		for _, level := range validLevels {
			t.Run(level, func(t *testing.T) {
				opts := &Options{
					Level:         level,
					Format:        "json",
					EnableConsole: false,
				}
				require.NoError(t, Init(opts))
			})
		}

		invalidLevels := []string{
			"INFO ", "DEBUG ", "inf", "deb", "INFOO", "DEBUGG",
			"WARNING", "ERR", "trace", "TRACE", " ", "_", "invalid",
		}

		for _, level := range invalidLevels {
			t.Run(level, func(t *testing.T) {
				opts := &Options{
					Level:         level,
					Format:        "json",
					EnableConsole: false,
				}
				require.Error(t, Init(opts))
			})
		}
	})

	t.Run("filter", func(t *testing.T) {
		logFile := filepath.Join(tmpDir, "filter.log")
		tests := []struct {
			level string
			logs  []struct {
				level   string
				visible bool
			}
		}{
			{
				level: "info",
				logs: []struct {
					level   string
					visible bool
				}{
					{"debug", false},
					{"info", true},
					{"warn", true},
					{"error", true},
				},
			},
			{
				level: "warn",
				logs: []struct {
					level   string
					visible bool
				}{
					{"debug", false},
					{"info", false},
					{"warn", true},
					{"error", true},
				},
			},
			{
				level: "error",
				logs: []struct {
					level   string
					visible bool
				}{
					{"debug", false},
					{"info", false},
					{"warn", false},
					{"error", true},
				},
			},
		}

		for _, tc := range tests {
			t.Run(tc.level, func(t *testing.T) {
				opts := &Options{
					Level:         tc.level,
					Format:        "json",
					Filename:      logFile,
					EnableConsole: false,
				}
				require.NoError(t, Init(opts))

				for _, log := range tc.logs {
					msg := fmt.Sprintf("test-%s", log.level)
					switch log.level {
					case "debug":
						Debug(msg)
					case "info":
						Info(msg)
					case "warn":
						Warn(msg)
					case "error":
						Error(msg)
					}
				}
				Close()

				content, err := os.ReadFile(logFile)
				require.NoError(t, err)

				for _, log := range tc.logs {
					msg := fmt.Sprintf("test-%s", log.level)
					exists := strings.Contains(string(content), msg)
					require.Equal(t, log.visible, exists)
				}

				os.Remove(logFile)
			})
		}
	})
}
