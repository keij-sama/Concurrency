// database/storage/storage.go
package storage

import (
	"context"
	"errors"

	"github.com/keij-sama/Concurrency/database/internal/database/storage/engine"
	"github.com/keij-sama/Concurrency/database/internal/database/storage/wal"
	"github.com/keij-sama/Concurrency/pkg/logger"
	"go.uber.org/zap"
)

// Storage определяет интерфейс для хранилища
type Storage interface {
	Set(key, value string) error
	Get(key string) (string, error)
	Delete(key string) error
	Close() error
}

// SimpleStorage реализует интерфейс Storage
type SimpleStorage struct {
	engine engine.Engine
	logger logger.Logger
	wal    *wal.WAL
	ctx    context.Context
	cancel context.CancelFunc
}

// NewStorage создает новое хранилище
func NewStorage(eng engine.Engine, log logger.Logger, walConfig *wal.WALConfig) (Storage, error) {
	ctx, cancel := context.WithCancel(context.Background())

	storage := &SimpleStorage{
		engine: eng,
		logger: log,
		ctx:    ctx,
		cancel: cancel,
	}

	// Если WAL включен, инициализируем его
	if walConfig != nil && walConfig.Enabled {
		walInstance, err := wal.NewWAL(*walConfig, log)
		if err != nil {
			cancel()
			return nil, err
		}

		storage.wal = walInstance

		// Восстанавливаем данные из WAL
		logs, err := walInstance.Recover()
		if err != nil {
			cancel()
			return nil, err
		}

		// Применяем операции из WAL
		for _, log := range logs {
			switch log.Operation {
			case "SET":
				if len(log.Args) >= 2 {
					if err := eng.Set(log.Args[0], log.Args[1]); err != nil {
						storage.logger.Error("Не удалось восстановить операцию SET",
							zap.Uint64("lsn", log.LSN),
							zap.String("key", log.Args[0]),
							zap.Error(err),
						)
					}
				}
			case "DEL":
				if len(log.Args) >= 1 {
					if err := eng.Delete(log.Args[0]); err != nil && !errors.Is(err, engine.ErrKeyNotFound) {
						storage.logger.Error("Не удалось восстановить операцию DEL",
							zap.Uint64("lsn", log.LSN),
							zap.String("key", log.Args[0]),
							zap.Error(err),
						)
					}
				}
			}
		}

		// Запускаем WAL
		walInstance.Start(ctx)
	}

	return storage, nil
}

// Set сохраняет пару ключ-значение
func (s *SimpleStorage) Set(key, value string) error {
	// Если WAL включен, сначала записываем в WAL
	if s.wal != nil {
		// Ждем подтверждения записи в WAL
		doneCh := s.wal.Set(key, value)

		// Ждем завершения операции WAL
		if err := <-doneCh; err != nil {
			s.logger.Error("Не удалось записать в WAL",
				zap.String("operation", "SET"),
				zap.String("key", key),
				zap.Error(err),
			)
			return err
		}
	}

	// Затем записываем в движок
	err := s.engine.Set(key, value)
	if err != nil {
		s.logger.Error("Не удалось установить значение в хранилище",
			zap.String("key", key),
			zap.Error(err),
		)
		return err
	}

	s.logger.Info("Значение установлено в хранилище",
		zap.String("key", key),
		zap.Int("value_length", len(value)),
	)

	return nil
}

// Get получает значение по ключу
func (s *SimpleStorage) Get(key string) (string, error) {
	value, err := s.engine.Get(key)
	if err != nil {
		if errors.Is(err, engine.ErrKeyNotFound) {
			s.logger.Info("Ключ не найден в хранилище",
				zap.String("key", key),
			)
		} else {
			s.logger.Error("Не удалось получить значение из хранилища",
				zap.String("key", key),
				zap.Error(err),
			)
		}
		return "", err
	}

	s.logger.Info("Значение получено из хранилища",
		zap.String("key", key),
	)

	return value, nil
}

// Delete удаляет пару ключ-значение
func (s *SimpleStorage) Delete(key string) error {
	// Если WAL включен, сначала записываем в WAL
	if s.wal != nil {
		// Ждем подтверждения записи в WAL
		doneCh := s.wal.Del(key)

		// Ждем завершения операции WAL
		if err := <-doneCh; err != nil {
			s.logger.Error("Не удалось записать в WAL",
				zap.String("operation", "DEL"),
				zap.String("key", key),
				zap.Error(err),
			)
			return err
		}
	}

	// Затем удаляем из движка
	err := s.engine.Delete(key)
	if err != nil {
		s.logger.Error("Не удалось удалить ключ из хранилища",
			zap.String("key", key),
			zap.Error(err),
		)
		return err
	}

	s.logger.Info("Ключ удален из хранилища",
		zap.String("key", key),
	)

	return nil
}

// Close закрывает хранилище
func (s *SimpleStorage) Close() error {
	// Отменяем контекст для завершения WAL
	s.cancel()

	// Закрываем WAL, если он включен
	if s.wal != nil {
		return s.wal.Close()
	}

	return nil
}
