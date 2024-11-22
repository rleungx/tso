package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	// L is the global logger instance
	L *zap.Logger
)

// Options represents the logging configuration options
type Options struct {
	// Log level: debug, info, warn, error, fatal
	Level string `mapstructure:"level"`
	// Log format: console, json
	Format string `mapstructure:"format"`
	// Log file path
	Filename string `mapstructure:"filename"`
	// Maximum size of a single log file in MB
	MaxSize int `mapstructure:"max-size"`
	// Maximum number of old files to retain
	MaxBackups int `mapstructure:"max-backups"`
	// Maximum number of days to retain old files
	MaxAge int `mapstructure:"max-age"`
	// Whether to compress old files
	Compress bool `mapstructure:"compress"`
	// Whether to enable development mode
	Development bool `mapstructure:"development"`
	// Whether to output to console
	EnableConsole bool `mapstructure:"enable-console"`
	// Whether to log caller information
	EnableCaller bool `mapstructure:"enable-caller"`
	// Whether to log stacktrace
	EnableStacktrace bool `mapstructure:"enable_stacktrace"`
}

// DefaultOptions returns the default logging configuration
func DefaultOptions() *Options {
	return &Options{
		Level:            "info",
		Format:           "console",
		MaxSize:          100,
		MaxBackups:       3,
		MaxAge:           7,
		Compress:         true,
		Development:      false,
		EnableConsole:    true,
		EnableCaller:     true,
		EnableStacktrace: true,
	}
}

// Init initializes the logging configuration
func Init(opts *Options) error {
	if opts == nil {
		opts = DefaultOptions()
	}

	// Parse log level
	level, err := parseLevel(opts.Level)
	if err != nil {
		return err
	}

	// Create encoder configuration
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     timeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	// Set encoder configuration for development mode
	if opts.Development {
		encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}

	// Create core configuration
	var cores []zapcore.Core

	// Add file output
	if opts.Filename != "" {
		// Ensure log directory exists
		if err := ensureDir(opts.Filename); err != nil {
			return err
		}

		// Configure log rotation
		writer := &lumberjack.Logger{
			Filename:   opts.Filename,
			MaxSize:    opts.MaxSize,
			MaxBackups: opts.MaxBackups,
			MaxAge:     opts.MaxAge,
			Compress:   opts.Compress,
		}

		var encoder zapcore.Encoder
		if opts.Format == "json" {
			encoder = zapcore.NewJSONEncoder(encoderConfig)
		} else {
			encoder = zapcore.NewConsoleEncoder(encoderConfig)
		}

		cores = append(cores, zapcore.NewCore(
			encoder,
			zapcore.AddSync(writer),
			level,
		))
	}
	if opts.Filename != "" {
		opts.EnableConsole = false
	}

	// Add console output
	if opts.EnableConsole {
		var encoder zapcore.Encoder
		if opts.Format == "json" {
			encoder = zapcore.NewJSONEncoder(encoderConfig)
		} else {
			encoder = zapcore.NewConsoleEncoder(encoderConfig)
		}

		cores = append(cores, zapcore.NewCore(
			encoder,
			zapcore.AddSync(os.Stdout),
			level,
		))
	}

	// Combine all cores
	core := zapcore.NewTee(cores...)

	// Create logger
	logger := zap.New(core)

	// Add options
	if opts.EnableCaller {
		logger = logger.WithOptions(zap.AddCaller())
	}
	if opts.EnableStacktrace {
		logger = logger.WithOptions(zap.AddStacktrace(zapcore.ErrorLevel))
	}
	if opts.Development {
		logger = logger.WithOptions(zap.Development())
	}

	// Set global logger
	L = logger
	return nil
}

// Parse log level
func parseLevel(level string) (zapcore.Level, error) {
	switch level {
	case "debug":
		return zapcore.DebugLevel, nil
	case "info":
		return zapcore.InfoLevel, nil
	case "warn":
		return zapcore.WarnLevel, nil
	case "error":
		return zapcore.ErrorLevel, nil
	case "fatal":
		return zapcore.FatalLevel, nil
	default:
		return zapcore.InfoLevel, fmt.Errorf("unknown level: %s", level)
	}
}

// Custom time encoder
func timeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format("2006-01-02 15:04:05.000"))
}

// Ensure directory exists
func ensureDir(filename string) error {
	dir := filepath.Dir(filename)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return os.MkdirAll(dir, 0755)
	}
	return nil
}

// Debug logs a message at DebugLevel
func Debug(msg string, fields ...zap.Field) {
	L.Debug(msg, fields...)
}

// Info logs a message at InfoLevel
func Info(msg string, fields ...zap.Field) {
	L.Info(msg, fields...)
}

// Warn logs a message at WarnLevel
func Warn(msg string, fields ...zap.Field) {
	L.Warn(msg, fields...)
}

// Error logs a message at ErrorLevel
func Error(msg string, fields ...zap.Field) {
	L.Error(msg, fields...)
}

// Fatal logs a message at FatalLevel
func Fatal(msg string, fields ...zap.Field) {
	L.Fatal(msg, fields...)
}

// With creates a logger with additional fields
func With(fields ...zap.Field) *zap.Logger {
	return L.With(fields...)
}

// Sync flushes any buffered log entries
func Sync() error {
	return L.Sync()
}
