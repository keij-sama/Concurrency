package storage

import (
	"github.com/keij-sama/Concurrency/database/internal/database/storage/engine"
	"github.com/keij-sama/Concurrency/pkg/logger"
	"go.uber.org/zap"
)

// Интерфейс для слоя хранения
type Storage interface {
	Set(key, value string) error
	Get(key string) (string, error)
	Delete(key string) error
}

// Хранилище
type SimpleStorage struct {
	engine engine.Engine
	logger logger.Logger
}

// Новое хранилище
func NewStorage(eng engine.Engine, log logger.Logger) Storage {
	return &SimpleStorage{
		engine: eng,
		logger: log,
	}
}

// Сохраняет значение
func (s *SimpleStorage) Set(key, value string) error {
	s.logger.Info("Setting value in storage",
		zap.String("key", key),
		zap.Int("value_length", len(value)),
	)

	err := s.engine.Set(key, value)
	if err != nil {
		s.logger.Error("Failed to set value in storage",
			zap.String("key", key),
			zap.Error(err),
		)
		return err
	}
	return nil
}

func (s *SimpleStorage) Get(key string) (string, error) {
	s.logger.Info("Getting value from storage",
		zap.String("key", key),
	)

	value, err := s.engine.Get(key)
	if err != nil {
		s.logger.Error("Failed to get value from storage",
			zap.String("key", key),
			zap.Error(err),
		)
		return "", err
	}
	return value, nil
}

// Удаляет значение
func (s *SimpleStorage) Delete(key string) error {
	s.logger.Info("Deleting key from storage",
		zap.String("key", key),
	)

	err := s.engine.Delete(key)
	if err != nil {
		s.logger.Error("Failed to delete key from storage",
			zap.String("key", key),
			zap.Error(err),
		)
		return err
	}
	return nil
}
