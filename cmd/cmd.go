package cmd

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/rleungx/tso/config"
	"github.com/rleungx/tso/logger"
	"github.com/rleungx/tso/server"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func Execute() {
	v := viper.New()

	// Create root command
	rootCmd := &cobra.Command{
		Use:   "tso-server",
		Short: "Start the timestamp server",
		Run: func(cmd *cobra.Command, args []string) {
			if err := v.BindPFlags(cmd.Flags()); err != nil {
				cmd.PrintErr("Failed to bind flags", zap.Error(err))
				os.Exit(0)
			}
			cfg, err := config.LoadConfig(v) // Load configuration from file and command line
			if err != nil {
				cmd.PrintErr("Failed to load configuration", zap.Error(err))
				os.Exit(0)
			}
			if err := logger.Init(cfg.Logger); err != nil {
				cmd.PrintErr("Failed to initialize logger", zap.Error(err))
				os.Exit(0)
			}
			defer logger.Sync()

			logger.Info("Starting server", zap.Any("config", cfg))
			srv := server.NewServer(cfg) // Create server instance, assuming NewServer is a constructor
			sigChan := make(chan os.Signal, 1)
			// Listen for system interrupt and terminate signals
			signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
			go func() {
				<-sigChan
				srv.Stop()
				os.Exit(0)
			}()
			// Start the server
			if err := srv.Start(); err != nil {
				logger.Fatal("Failed to start server", zap.Error(err)) // Use zap to log fatal errors
			}
		},
	}
	flags := rootCmd.Flags()
	flags.String("host", "", "Host address")
	flags.Int("port", 0, "Port number")
	flags.String("cacert-file", "", "Path to CA certificate file")
	flags.String("cert-file", "", "Path to TLS certificate file")
	flags.String("key-file", "", "Path to TLS key file")
	flags.String("config", "", "Path to configuration file")

	if err := rootCmd.Execute(); err != nil {
		logger.Fatal("Failed to execute command", zap.Error(err)) // Use zap to log fatal errors
	}
}
