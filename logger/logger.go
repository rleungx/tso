package logger

import (
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// L is the global logger instance
var L *zap.Logger

// Options simplified configuration options
type Options struct {
	Level         string `mapstructure:"level"`
	Format        string `mapstructure:"format"`
	Filename      string `mapstructure:"filename"`
	MaxSize       int    `mapstructure:"max-size"`
	MaxBackups    int    `mapstructure:"max-backups"`
	MaxAge        int    `mapstructure:"max-age"`
	Compress      bool   `mapstructure:"compress"`
	EnableConsole bool   `mapstructure:"enable-console"`
}

// DefaultOptions returns the default configuration
func DefaultOptions() *Options {
	return &Options{
		Level:         "info",
		Format:        "console",
		MaxSize:       100,
		MaxBackups:    3,
		MaxAge:        7,
		Compress:      true,
		EnableConsole: true,
	}
}

// Init simplified initialization function
func Init(opts *Options) error {
	if opts == nil {
		opts = DefaultOptions()
	}

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     timeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	zapLevel := zap.NewAtomicLevel()
	if err := zapLevel.UnmarshalText([]byte(opts.Level)); err != nil {
		return fmt.Errorf("invalid log level: %s", opts.Level)
	}

	var cores []zapcore.Core

	// Create encoder
	encoder := zapcore.NewConsoleEncoder(encoderConfig)
	if opts.Format == "json" {
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	}

	// Add file output
	if opts.Filename != "" {
		writer := &lumberjack.Logger{
			Filename:   opts.Filename,
			MaxSize:    opts.MaxSize,
			MaxBackups: opts.MaxBackups,
			MaxAge:     opts.MaxAge,
			Compress:   opts.Compress,
		}
		cores = append(cores, zapcore.NewCore(encoder, zapcore.AddSync(writer), zapLevel))
	} else if opts.EnableConsole {
		cores = append(cores, zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), zapLevel))
	}

	L = zap.New(zapcore.NewTee(cores...))
	return nil
}

// Custom time encoder
func timeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format("2006-01-02 15:04:05.000"))
}

// Simplified log method
func log(level zapcore.Level, msg string, fields ...zap.Field) {
	if L == nil {
		return
	}
	switch level {
	case zapcore.DebugLevel:
		L.Debug(msg, fields...)
	case zapcore.InfoLevel:
		L.Info(msg, fields...)
	case zapcore.WarnLevel:
		L.Warn(msg, fields...)
	case zapcore.ErrorLevel:
		L.Error(msg, fields...)
	case zapcore.FatalLevel:
		L.Fatal(msg, fields...)
	}
}

// Debug logs a message at DebugLevel
func Debug(msg string, fields ...zap.Field) {
	log(zapcore.DebugLevel, msg, fields...)
}

// Info logs a message at InfoLevel
func Info(msg string, fields ...zap.Field) {
	log(zapcore.InfoLevel, msg, fields...)
}

// Warn logs a message at WarnLevel
func Warn(msg string, fields ...zap.Field) {
	log(zapcore.WarnLevel, msg, fields...)
}

// Error logs a message at ErrorLevel
func Error(msg string, fields ...zap.Field) {
	log(zapcore.ErrorLevel, msg, fields...)
}

// Fatal logs a message at FatalLevel
func Fatal(msg string, fields ...zap.Field) {
	log(zapcore.FatalLevel, msg, fields...)
}

// With creates a logger with additional fields
func With(fields ...zap.Field) *zap.Logger {
	if L == nil {
		return nil
	}
	return L.With(fields...)
}

// Sync flushes any buffered log entries
func Sync() error {
	if L == nil {
		return nil
	}
	return L.Sync()
}

// Close closes the logger and releases resources
func Close() error {
	if L == nil {
		return nil
	}
	err := L.Sync()
	L = nil
	return err
}
