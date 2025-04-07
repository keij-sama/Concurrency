package compute

import (
	"fmt"

	"github.com/keij-sama/Concurrency/database/internal/database/compute/parser"
	"github.com/keij-sama/Concurrency/database/internal/database/storage"
	"github.com/keij-sama/Concurrency/pkg/logger"
	"go.uber.org/zap"
)

// Compute определяет интерфейс для обработки запросов
type Compute interface {
	Process(input string) (string, error)
}

// SimpleCompute реализует интерфейс Compute
type SimpleCompute struct {
	parser  parser.Parser
	storage storage.Storage
	logger  logger.Logger
}

// NewCompute создает новый экземпляр обработчика запросов
func NewCompute(p parser.Parser, s storage.Storage, log logger.Logger) Compute {
	return &SimpleCompute{
		parser:  p,
		storage: s,
		logger:  log,
	}
}

// Process обрабатывает запрос
func (c *SimpleCompute) Process(input string) (string, error) {
	c.logger.Info("Processing request",
		zap.String("input", input),
	)

	// Парсинг запроса
	cmd, err := c.parser.Parse(input)
	if err != nil {
		c.logger.Error("Parse error",
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
		return "", fmt.Errorf("unknown command: %s", cmd.Type)
	}
}
