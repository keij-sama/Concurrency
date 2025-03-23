package storage

import (
	"github.com/keij-sama/Concurrency/database/internal/database/storage/engine"
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
	logger *zap.Logger
}

// Новое хранилище
func NewStorage(eng engine.Engine, logger *zap.Logger) Storage {

	// Если логгер не передан, создаем дефолтный
	if logger == nil {
		logger, _ = zap.NewDevelopment()
	}

	return &SimpleStorage{
		engine: eng,
		logger: logger,
	}
}

// Сохраняет значение
func (s *SimpleStorage) Set(key, value string) error {
	s.logger.Info("saving value",
		zap.String("key", key),
		zap.Int("value_lenght", len(value)),
	)
	return s.engine.Set(key, value)
}

func (s *SimpleStorage) Get(key string) (string, error) {
	s.logger.Info("get value",
		zap.String("key", key),
	)
	value, err := s.engine.Get(key)
	if err != nil {
		if err == engine.ErrKeyNotFound {
			s.logger.Info("key not found", zap.String("key", key))
		} else {
			s.logger.Error("Error when receiving a value",
				zap.String("key", key),
				zap.Error(err),
			)
		}
		return "", err
	}
	return value, nil
}

// Удаляет значение
func (s *SimpleStorage) Delete(key string) error {
	s.logger.Info("Delete value",
		zap.String("key", key),
	)

	err := s.engine.Delete(key)
	if err != nil {
		if err == engine.ErrKeyNotFound {
			s.logger.Info("Key not found for deletion", zap.String("key", key))
		} else {
			s.logger.Error("Error when deleting a value",
				zap.String("key", key),
				zap.Error(err),
			)
		}
		return err
	}
	return nil
}
