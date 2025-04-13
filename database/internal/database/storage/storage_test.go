// database/storage/storage_test.go
package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/keij-sama/Concurrency/database/internal/database/storage/engine"
	"github.com/keij-sama/Concurrency/database/internal/database/storage/wal"
	"github.com/keij-sama/Concurrency/pkg/logger"
	"go.uber.org/zap"
)

func TestStorageWithWAL(t *testing.T) {
	// Создаем временную директорию для тестов
	tempDir, err := os.MkdirTemp("", "storage_wal_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Создаем конфигурацию WAL
	walConfig := &wal.WALConfig{
		Enabled:              true,
		FlushingBatchSize:    2,
		FlushingBatchTimeout: 10 * time.Millisecond,
		MaxSegmentSize:       1024, // 1KB для быстрых тестов
		DataDirectory:        tempDir,
	}

	// Создаем логгер
	zapLogger, _ := zap.NewDevelopment()
	customLogger := logger.NewLoggerWithZap(zapLogger)

	// Создаем движок
	eng := engine.NewInMemoryEngine()

	// Создаем хранилище с WAL
	storage, err := NewStorage(eng, customLogger, StorageOptions{
		WALConfig:         walConfig,
		ReplicationConfig: nil,
	})
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Выполняем операции с хранилищем
	if err := storage.Set("key1", "value1"); err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	if err := storage.Set("key2", "value2"); err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	// Проверяем, что значения установлены
	value1, err := storage.Get("key1")
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	if value1 != "value1" {
		t.Errorf("Expected value1, got %s", value1)
	}

	// Удаляем значение
	if err := storage.Delete("key1"); err != nil {
		t.Fatalf("Failed to delete value: %v", err)
	}

	// Проверяем, что значение удалено
	_, err = storage.Get("key1")
	if !errors.Is(err, engine.ErrKeyNotFound) {
		t.Errorf("Expected key not found error, got %v", err)
	}

	// Закрываем хранилище
	if err := storage.Close(); err != nil {
		t.Fatalf("Failed to close storage: %v", err)
	}

	// Проверяем, что WAL файлы созданы
	files, err := filepath.Glob(filepath.Join(tempDir, "wal_*.log"))
	if err != nil {
		t.Fatalf("Failed to list WAL files: %v", err)
	}

	if len(files) == 0 {
		t.Errorf("Expected WAL files to be created, but none found")
	}

	// Создаем новое хранилище с тем же WAL для проверки восстановления
	newEngine := engine.NewInMemoryEngine()
	newStorage, err := NewStorage(newEngine, customLogger, StorageOptions{
		WALConfig:         walConfig, // Важно использовать ту же конфигурацию WAL
		ReplicationConfig: nil,
	})
	if err != nil {
		t.Fatalf("Failed to create new storage: %v", err)
	}

	// Проверяем, что данные восстановлены
	value2, err := newStorage.Get("key2")
	if err != nil {
		t.Fatalf("Failed to get value after recovery: %v", err)
	}
	if value2 != "value2" {
		t.Errorf("Expected value2 after recovery, got %s", value2)
	}

	// Проверяем, что удаленные данные не восстановлены
	_, err = newStorage.Get("key1")
	if !errors.Is(err, engine.ErrKeyNotFound) {
		t.Errorf("Expected key1 to remain deleted after recovery, got %v", err)
	}

	// Закрываем второе хранилище
	if err := newStorage.Close(); err != nil {
		t.Fatalf("Failed to close new storage: %v", err)
	}
}

func TestStorageWithoutWAL(t *testing.T) {
	// Создаем логгер
	zapLogger, _ := zap.NewDevelopment()
	customLogger := logger.NewLoggerWithZap(zapLogger)

	// Создаем движок
	eng := engine.NewInMemoryEngine()

	// Создаем хранилище без WAL
	storage, err := NewStorage(eng, customLogger, StorageOptions{})
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Выполняем операции с хранилищем
	if err := storage.Set("key1", "value1"); err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	// Проверяем, что значение установлено
	value, err := storage.Get("key1")
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	if value != "value1" {
		t.Errorf("Expected value1, got %s", value)
	}

	// Удаляем значение
	if err := storage.Delete("key1"); err != nil {
		t.Fatalf("Failed to delete value: %v", err)
	}

	// Проверяем, что значение удалено
	_, err = storage.Get("key1")
	if !errors.Is(err, engine.ErrKeyNotFound) {
		t.Errorf("Expected key not found error, got %v", err)
	}

	// Закрываем хранилище
	if err := storage.Close(); err != nil {
		t.Fatalf("Failed to close storage: %v", err)
	}
}
