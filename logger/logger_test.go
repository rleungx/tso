package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"
)

func TestMain(m *testing.M) {
	opts := []goleak.Option{
		// see https://github.com/natefinch/lumberjack/issues/56
		goleak.IgnoreTopFunction("gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun"),
	}
	goleak.VerifyTestMain(m, opts...)
}

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

func TestLoggerInitialization(t *testing.T) {
	// Test default configuration
	t.Run("default options", func(t *testing.T) {
		err := Init(nil)
		require.NoError(t, err)
		require.NotNil(t, L)
		Close()
	})

	// Test invalid log level
	t.Run("invalid log level", func(t *testing.T) {
		opts := DefaultOptions()
		opts.Level = "invalid"
		err := Init(opts)
		require.Error(t, err)
	})

	// Test file output
	t.Run("file output", func(t *testing.T) {
		opts := DefaultOptions()
		opts.Filename = "test.log"
		err := Init(opts)
		require.NoError(t, err)
		Info("test message")
		require.FileExists(t, "test.log")
		Close()
		os.Remove("test.log")
	})

	// Test JSON format
	t.Run("json format", func(t *testing.T) {
		opts := DefaultOptions()
		opts.Format = "json"
		err := Init(opts)
		require.NoError(t, err)
		Close()
	})
}

func TestLogMethods(t *testing.T) {
	Init(nil)
	defer Close()

	// Test all log levels
	t.Run("log levels", func(t *testing.T) {
		Debug("debug message")
		Info("info message")
		Warn("warn message")
		Error("error message")
		// Fatal is not tested because it would cause the program to exit
	})

	// Test log with fields
	t.Run("log with fields", func(t *testing.T) {
		Info("message with fields",
			zap.String("key", "value"),
			zap.Int("count", 1))
	})
}

func TestWithFields(t *testing.T) {
	// Test With method
	t.Run("logger with fields", func(t *testing.T) {
		Init(nil)
		logger := With(zap.String("service", "test"))
		require.NotNil(t, logger)
		Close()
	})

	// Test With method on nil logger
	t.Run("with on nil logger", func(t *testing.T) {
		L = nil
		logger := With(zap.String("service", "test"))
		require.Nil(t, logger)
	})
}

func TestSync(t *testing.T) {
	// Test Sync method - using file instead of stdout
	t.Run("sync logger", func(t *testing.T) {
		opts := DefaultOptions()
		opts.Filename = "test.log" // Using file output
		opts.EnableConsole = false // Disabling console output

		err := Init(opts)
		require.NoError(t, err)

		Info("test message")
		err = Sync()
		require.NoError(t, err)

		Close()
		os.Remove("test.log")
	})

	// Test Sync method on nil logger
	t.Run("sync nil logger", func(t *testing.T) {
		L = nil
		err := Sync()
		require.NoError(t, err)
	})
}
