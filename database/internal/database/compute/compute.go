package compute

import (
	"fmt"

	"github.com/keij-sama/Concurrency/database/internal/database/compute/parser"
	"github.com/keij-sama/Concurrency/database/internal/database/storage"
	"go.uber.org/zap"
)

// Интерфейс для обработки запросов
type Compute interface {
	Process(input string) (string, error)
}

// Реализация для обработчика запросов
type SimpleCompute struct {
	parser  parser.Parser
	storage storage.Storage
	logger  *zap.Logger
}

// Новый обработчик запросов
func NewCompute(p parser.Parser, s storage.Storage) Compute {
	// Простой логгер
	logger, _ := zap.NewDevelopment()

	return &SimpleCompute{
		parser:  p,
		storage: s,
		logger:  logger,
	}
}

// Обработка запроса
func (c *SimpleCompute) Process(input string) (string, error) {
	c.logger.Info("Request processing", zap.String("input", input))

	// Парсинг запроса
	cmd, err := c.parser.Parse(input)
	if err != nil {
		c.logger.Error("Parser error",
			zap.String("input", input),
			zap.Error(err),
		)
		return "", err
	}

	// Обработка команды
	switch cmd.Type {
	case parser.CommandSet:
		key, value := cmd.Arguments[0], cmd.Arguments[1]
		err = c.storage.Set(key, value)
		if err != nil {
			return "", err
		}
		return "OK", nil

	case parser.CommandGet:
		key := cmd.Arguments[0]
		value, err := c.storage.Get(key)
		if err != nil {
			return "", err
		}
		return value, nil

	case parser.CommandDel:
		key := cmd.Arguments[0]
		err = c.storage.Delete(key)
		if err != nil {
			return "", err
		}
		return "OK", nil

	default:
		return "", fmt.Errorf("неизвестная команда: %s", cmd.Type)
	}
}
