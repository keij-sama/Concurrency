package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/keij-sama/Concurrency/database/internal/config"
	"github.com/keij-sama/Concurrency/database/internal/database/compute"
	"github.com/keij-sama/Concurrency/database/internal/database/compute/parser"
	"github.com/keij-sama/Concurrency/database/internal/database/storage"
	"github.com/keij-sama/Concurrency/database/internal/database/storage/engine"
	"github.com/keij-sama/Concurrency/pkg/logger"
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
	customLogger := logger.NewLogger()

	// Инициализация компонентов
	parser := parser.NewParser()
	engine := engine.NewInMemoryEngine()

	// Получаем конфигурацию WAL
	walConfig := cfg.GetWALConfig()

	// Инициализируем хранилище с WAL, если он включен
	storage, err := storage.NewStorage(engine, customLogger, storage.StorageOptions{
		WALConfig:         walConfig,
		ReplicationConfig: nil,
	})
	if err != nil {
		fmt.Printf("ERROR: Failed to initialize storage: %v\n", err)
		os.Exit(1)
	}
	defer storage.Close()

	compute := compute.NewCompute(parser, storage, customLogger)

	fmt.Println("In-memory Key-Value Database")
	if walConfig != nil && walConfig.Enabled {
		fmt.Println("WAL is enabled - data will persist after restart")
	} else {
		fmt.Println("WAL is disabled - data will be lost after restart")
	}
	fmt.Println("Available commands: SET, GET, DEL")
	fmt.Println("To exit, type exit or quit")
	fmt.Println()

	// Цикл обработки команд
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		input := scanner.Text()
		input = strings.TrimSpace(input)

		if input == "" {
			continue
		}

		// Проверка команды выхода
		if strings.ToLower(input) == "exit" || strings.ToLower(input) == "quit" {
			fmt.Println("Finishing work")
			break
		}

		// Обработка команды
		result, err := compute.Process(input)
		if err != nil {
			fmt.Printf("ERROR: %s\n", err)
		} else {
			fmt.Println(result)
		}
	}
}
