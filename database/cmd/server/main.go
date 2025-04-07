// cmd/server/main.go
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
	"github.com/keij-sama/Concurrency/pkg/logger"
	"go.uber.org/zap"
)

func main() {
	// Парсим флаги командной строки
	configPath := flag.String("config", "config.yaml", "Path to config file")
	flag.Parse()

	// Загружаем конфигурацию
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		fmt.Printf("Warning: Could not load config file: %v. Using default configuration.\n", err)
	}

	// Создаем логгер
	zapLogger, _ := zap.NewProduction()
	if cfg.Logging.Level == "debug" {
		zapLogger, _ = zap.NewDevelopment()
	}
	defer zapLogger.Sync()

	customLogger := logger.NewLoggerWithZap(zapLogger)

	// Инициализируем компоненты базы данных
	parser := parser.NewParser()
	eng := engine.NewInMemoryEngine()

	// Получаем конфигурацию WAL
	walConfig := cfg.GetWALConfig()

	// Инициализируем хранилище с WAL
	storage, err := storage.NewStorage(eng, customLogger, walConfig)
	if err != nil {
		zapLogger.Fatal("Failed to initialize storage", zap.Error(err))
	}
	defer storage.Close()

	// Инициализируем обработчик запросов
	compute := compute.NewCompute(parser, storage, customLogger)

	// Создаем TCP-сервер
	var bufferSize int
	if cfg.Network.MaxMessageSize != "" {
		fmt.Sscanf(cfg.Network.MaxMessageSize, "%dKB", &bufferSize)
		bufferSize = bufferSize << 10
	}
	if bufferSize == 0 {
		bufferSize = 4 << 10
	}

	server, err := network.NewTCPServer(
		cfg.Network.Address,
		zapLogger,
		network.WithMaxConnections(cfg.Network.MaxConnections),
		network.WithIdleTimeout(cfg.Network.IdleTimeout),
		network.WithBufferSize(bufferSize),
	)
	if err != nil {
		zapLogger.Fatal("Failed to create server", zap.Error(err))
	}

	// Создаем контекст с отменой
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Настраиваем обработку сигналов
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		zapLogger.Info("Shutting down server...")
		cancel()
	}()

	// Запускаем сервер
	zapLogger.Info("Starting server", zap.String("address", cfg.Network.Address))
	server.HandleQueries(ctx, func(ctx context.Context, query []byte) []byte {
		result, err := compute.Process(string(query))
		if err != nil {
			return []byte(fmt.Sprintf("ERROR: %s", err))
		}
		return []byte(result)
	})

	zapLogger.Info("Server stopped")
}
