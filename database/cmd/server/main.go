package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/keij-sama/Concurrency/database/internal/config"
	"github.com/keij-sama/Concurrency/database/internal/database/compute"
	"github.com/keij-sama/Concurrency/database/internal/database/compute/parser"
	"github.com/keij-sama/Concurrency/database/internal/database/storage"
	"github.com/keij-sama/Concurrency/database/internal/database/storage/engine"
	"github.com/keij-sama/Concurrency/database/internal/network"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	// Парсим флаги командной строки
	configPath := flag.String("config", "config.yaml", "Path to config file")
	flag.Parse()

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		fmt.Printf("Warning: Could not load config file: %v. Using default configuration.\n", err)
	}

	var level zapcore.Level
	switch cfg.Logging.Level {
	case "debug":
		level = zapcore.DebugLevel
	case "info":
		level = zapcore.InfoLevel
	case "warn":
		level = zapcore.WarnLevel
	case "error":
		level = zapcore.ErrorLevel
	default:
		level = zapcore.InfoLevel
	}

	logConfig := zap.NewProductionConfig()
	logConfig.Level = zap.NewAtomicLevelAt(level)
	if cfg.Logging.Output != "stdout" && cfg.Logging.Output != "" {
		logConfig.OutputPaths = []string{cfg.Logging.Output}
	}

	logger, err := logConfig.Build()
	if err != nil {
		fmt.Printf("Error creating logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	if logger == nil {
		fmt.Println("Logger is nil, creating default logger")
		logger, _ = zap.NewProduction()
	}

	// Создаем TCP-сервер
	// Преобразуем строку размера сообщения в байты
	var bufferSize int
	if cfg.Network.MaxMessageSize != "" {
		fmt.Sscanf(cfg.Network.MaxMessageSize, "%dKB", &bufferSize)
		bufferSize = bufferSize << 10 // Конвертируем KB в байты
	}
	if bufferSize == 0 {
		bufferSize = 4 << 10 // 4KB по умолчанию
	}

	// Создаем TCP-сервер
	server, err := network.NewTCPServer(
		cfg.Network.Address,
		logger,
		network.WithMaxConnections(cfg.Network.MaxConnections),
		network.WithIdleTimeout(cfg.Network.IdleTimeout),
		network.WithBufferSize(bufferSize),
	)
	if err != nil {
		logger.Fatal("Failed to create server", zap.Error(err))
	}

	// Инициализируем компоненты базы данных
	parser := parser.NewParser()
	eng := engine.NewInMemoryEngine()
	storage := storage.NewStorage(eng, logger)
	compute := compute.NewCompute(parser, storage, logger)

	// Создаем контекст с отменой
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Настраиваем обработку сигналов
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("Shutting down server...")
		cancel()
	}()

	// Запускаем сервер
	logger.Info("Starting server", zap.String("address", cfg.Network.Address))
	server.HandleQueries(ctx, func(ctx context.Context, query []byte) []byte {
		result, err := compute.Process(string(query))
		if err != nil {
			return []byte(fmt.Sprintf("ERROR: %s", err))
		}
		return []byte(result)
	})
	logger.Info("Server stopped")
}
