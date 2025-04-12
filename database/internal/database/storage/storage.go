// database/storage/storage.go
package storage

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/keij-sama/Concurrency/database/internal/network"
	"github.com/keij-sama/Concurrency/database/storage/engine"
	"github.com/keij-sama/Concurrency/database/storage/replication"
	"github.com/keij-sama/Concurrency/database/storage/wal"
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
	engine      engine.Engine
	logger      logger.Logger
	wal         *wal.WAL
	replication replication.Replication
	isMaster    bool
	ctx         context.Context
	cancel      context.CancelFunc
}

// StorageOptions содержит опции для создания хранилища
type StorageOptions struct {
	WALConfig         *wal.WALConfig
	ReplicationConfig *replication.ReplicationConfig
}

// NewStorage создает новое хранилище
func NewStorage(eng engine.Engine, log logger.Logger, options StorageOptions) (Storage, error) {
	ctx, cancel := context.WithCancel(context.Background())

	storage := &SimpleStorage{
		engine:   eng,
		logger:   log,
		ctx:      ctx,
		cancel:   cancel,
		isMaster: true, // По умолчанию считаем, что это мастер
	}

	// Инициализируем WAL, если он включен
	if options.WALConfig != nil && options.WALConfig.Enabled {
		walInstance, err := wal.NewWAL(*options.WALConfig, log)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to initialize WAL: %w", err)
		}

		storage.wal = walInstance

		// Восстанавливаем данные из WAL
		if err := storage.recoverFromWAL(); err != nil {
			cancel()
			return nil, fmt.Errorf("failed to recover from WAL: %w", err)
		}

		// Запускаем WAL
		walInstance.Start(ctx)
	}

	// Инициализируем репликацию, если она включена
	if options.ReplicationConfig != nil && options.ReplicationConfig.Enabled {
		// Проверяем, что WAL включен (требуется для репликации)
		if storage.wal == nil {
			cancel()
			return nil, errors.New("WAL must be enabled for replication")
		}

		repl, err := storage.initializeReplication(*options.ReplicationConfig)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to initialize replication: %w", err)
		}

		storage.replication = repl
		storage.isMaster = repl.IsMaster()
	}

	return storage, nil
}

// initializeReplication инициализирует репликацию
func (s *SimpleStorage) initializeReplication(cfg replication.ReplicationConfig) (replication.Replication, error) {
	if cfg.ReplicaType == replication.TypeMaster {
		// Настраиваем мастер
		// Создаем TCP сервер для репликации
		listener, err := net.Listen("tcp", cfg.MasterAddress)
		if err != nil {
			return nil, fmt.Errorf("failed to create replication listener: %w", err)
		}

		server := network.NewTCPServer(listener, s.logger)

		master, err := replication.NewMaster(server, s.wal.GetDirectory(), s.logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create replication master: %w", err)
		}

		// Запускаем мастер
		if err := master.Start(s.ctx); err != nil {
			return nil, fmt.Errorf("failed to start replication master: %w", err)
		}

		return master, nil
	} else {
		// Настраиваем слейв
		client, err := network.NewTCPClient(cfg.MasterAddress)
		if err != nil {
			return nil, fmt.Errorf("failed to create replication client: %w", err)
		}

		// Функция для восстановления из WAL
		walRecovery := func(logs []wal.Log) error {
			for _, log := range logs {
				switch log.Operation {
				case "SET":
					if len(log.Args) >= 2 {
						if err := s.engine.Set(log.Args[0], log.Args[1]); err != nil {
							s.logger.Error("Failed to apply SET operation from WAL",
								zap.Uint64("lsn", log.LSN),
								zap.String("key", log.Args[0]),
								zap.Error(err),
							)
						}
					}
				case "DEL":
					if len(log.Args) >= 1 {
						if err := s.engine.Delete(log.Args[0]); err != nil && !errors.Is(err, engine.ErrKeyNotFound) {
							s.logger.Error("Failed to apply DEL operation from WAL",
								zap.Uint64("lsn", log.LSN),
								zap.String("key", log.Args[0]),
								zap.Error(err),
							)
						}
					}
				}
			}
			return nil
		}

		slave, err := replication.NewSlave(client, s.wal.GetDirectory(), cfg.SyncInterval, s.logger, walRecovery)
		if err != nil {
			return nil, fmt.Errorf("failed to create replication slave: %w", err)
		}

		// Запускаем слейв
		if err := slave.Start(s.ctx); err != nil {
			return nil, fmt.Errorf("failed to start replication slave: %w", err)
		}

		return slave, nil
	}
}

// recoverFromWAL восстанавливает данные из WAL
func (s *SimpleStorage) recoverFromWAL() error {
	logs, err := s.wal.Recover()
	if err != nil {
		return err
	}

	// Применяем операции из WAL
	for _, log := range logs {
		switch log.Operation {
		case "SET":
			if len(log.Args) >= 2 {
				if err := s.engine.Set(log.Args[0], log.Args[1]); err != nil {
					s.logger.Error("Failed to restore SET operation",
						zap.Uint64("lsn", log.LSN),
						zap.String("key", log.Args[0]),
						zap.Error(err),
					)
				}
			}
		case "DEL":
			if len(log.Args) >= 1 {
				if err := s.engine.Delete(log.Args[0]); err != nil && !errors.Is(err, engine.ErrKeyNotFound) {
					s.logger.Error("Failed to restore DEL operation",
						zap.Uint64("lsn", log.LSN),
						zap.String("key", log.Args[0]),
						zap.Error(err),
					)
				}
			}
		}
	}

	return nil
}

// Set сохраняет пару ключ-значение
func (s *SimpleStorage) Set(key, value string) error {
	// Проверка, что это мастер (писать можно только в мастер)
	if !s.isMaster {
		return errors.New("write operations not allowed on slave replica")
	}

	// Если WAL включен, сначала записываем в WAL
	if s.wal != nil {
		// Ждем подтверждения записи в WAL
		done := s.wal.Set(key, value)

		// Ждем завершения операции WAL
		if err := <-done; err != nil {
			s.logger.Error("Failed to write to WAL",
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
		s.logger.Error("Failed to set value in storage",
			zap.String("key", key),
			zap.Error(err),
		)
		return err
	}

	s.logger.Info("Value set in storage",
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
			s.logger.Info("Key not found in storage",
				zap.String("key", key),
			)
		} else {
			s.logger.Error("Failed to get value from storage",
				zap.String("key", key),
				zap.Error(err),
			)
		}
		return "", err
	}

	s.logger.Info("Value retrieved from storage",
		zap.String("key", key),
	)

	return value, nil
}

// Delete удаляет пару ключ-значение
func (s *SimpleStorage) Delete(key string) error {
	// Проверка, что это мастер (писать можно только в мастер)
	if !s.isMaster {
		return errors.New("write operations not allowed on slave replica")
	}

	// Если WAL включен, сначала записываем в WAL
	if s.wal != nil {
		// Ждем подтверждения записи в WAL
		done := s.wal.Del(key)

		// Ждем завершения операции WAL
		if err := <-done; err != nil {
			s.logger.Error("Failed to write to WAL",
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
		s.logger.Error("Failed to delete key from storage",
			zap.String("key", key),
			zap.Error(err),
		)
		return err
	}

	s.logger.Info("Key deleted from storage",
		zap.String("key", key),
	)

	return nil
}

// Close закрывает хранилище
func (s *SimpleStorage) Close() error {
	// Отменяем контекст для остановки всех фоновых горутин
	s.cancel()

	// Закрываем WAL, если он включен
	if s.wal != nil {
		if err := s.wal.Close(); err != nil {
			s.logger.Error("Failed to close WAL", zap.Error(err))
		}
	}

	// Закрываем репликацию, если она включена
	if s.replication != nil {
		if err := s.replication.Close(); err != nil {
			s.logger.Error("Failed to close replication", zap.Error(err))
		}
	}

	return nil
}
