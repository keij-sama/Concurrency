// database/storage/wal/wal_test.go
package wal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/keij-sama/Concurrency/pkg/logger"
	"go.uber.org/zap"
)

func TestWAL(t *testing.T) {
	// Создаем временную директорию для тестов
	tempDir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Создаем конфигурацию WAL
	walConfig := WALConfig{
		Enabled:              true,
		FlushingBatchSize:    2,
		FlushingBatchTimeout: 10 * time.Millisecond,
		MaxSegmentSize:       1024, // 1KB для быстрых тестов
		DataDirectory:        tempDir,
	}

	// Создаем логгер
	zapLogger, _ := zap.NewDevelopment()
	customLogger := logger.NewLoggerWithZap(zapLogger)

	// Создаем WAL
	wal, err := NewWAL(walConfig, customLogger)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Запускаем WAL
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wal.Start(ctx)

	// Добавляем записи в WAL
	done1 := wal.Set("key1", "value1")
	if err := <-done1; err != nil {
		t.Fatalf("Failed to append SET to WAL: %v", err)
	}

	done2 := wal.Set("key2", "value2")
	if err := <-done2; err != nil {
		t.Fatalf("Failed to append SET to WAL: %v", err)
	}

	// Добавляем запись DEL
	done3 := wal.Del("key1")
	if err := <-done3; err != nil {
		t.Fatalf("Failed to append DEL to WAL: %v", err)
	}

	// Закрываем WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Проверяем, что файлы WAL созданы
	files, err := filepath.Glob(filepath.Join(tempDir, "wal_*.log"))
	if err != nil {
		t.Fatalf("Failed to list WAL files: %v", err)
	}

	if len(files) == 0 {
		t.Errorf("Expected WAL files to be created, but none found")
	}

	// Создаем новый WAL для восстановления данных
	newWAL, err := NewWAL(walConfig, customLogger)
	if err != nil {
		t.Fatalf("Failed to create new WAL: %v", err)
	}

	// Восстанавливаем данные из WAL
	logs, err := newWAL.Recover()
	if err != nil {
		t.Fatalf("Failed to restore from WAL: %v", err)
	}

	// Проверяем восстановленные записи
	if len(logs) != 3 {
		t.Errorf("Expected 3 records to be recovered, got %d", len(logs))
	}

	// Проверяем содержимое записей
	found := map[string]bool{
		"SET:key1:value1": false,
		"SET:key2:value2": false,
		"DEL:key1":        false,
	}

	for _, log := range logs {
		var key string
		switch log.Operation {
		case "SET":
			if len(log.Args) >= 2 {
				key = "SET:" + log.Args[0] + ":" + log.Args[1]
			}
		case "DEL":
			if len(log.Args) >= 1 {
				key = "DEL:" + log.Args[0]
			}
		}
		found[key] = true
	}

	for k, v := range found {
		if !v {
			t.Errorf("Expected record %s not found in recovered logs", k)
		}
	}

	// Закрываем новый WAL
	if err := newWAL.Close(); err != nil {
		t.Fatalf("Failed to close new WAL: %v", err)
	}
}

func TestWALSegmentation(t *testing.T) {
	// Создаем временную директорию для тестов
	tempDir, err := os.MkdirTemp("", "wal_segmentation_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Создаем конфигурацию WAL с маленьким размером сегмента
	walConfig := WALConfig{
		Enabled:              true,
		FlushingBatchSize:    1, // Запись каждого лога отдельно
		FlushingBatchTimeout: 5 * time.Millisecond,
		MaxSegmentSize:       100, // Очень маленький размер для быстрой сегментации
		DataDirectory:        tempDir,
	}

	// Создаем логгер
	zapLogger, _ := zap.NewDevelopment()
	customLogger := logger.NewLoggerWithZap(zapLogger)

	// Создаем WAL
	wal, err := NewWAL(walConfig, customLogger)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Запускаем WAL
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wal.Start(ctx)

	// Добавляем множество записей для создания нескольких сегментов
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)

		done := wal.Set(key, value)
		if err := <-done; err != nil {
			t.Fatalf("Failed to append SET to WAL: %v", err)
		}

		// Небольшая задержка для обработки записи
		time.Sleep(10 * time.Millisecond)
	}

	// Закрываем WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Проверяем, что создано несколько сегментов
	files, err := filepath.Glob(filepath.Join(tempDir, "wal_*.log"))
	if err != nil {
		t.Fatalf("Failed to list WAL files: %v", err)
	}

	if len(files) <= 1 {
		t.Errorf("Expected multiple WAL segments, got %d", len(files))
	}

	// Создаем новый WAL для восстановления данных
	newWAL, err := NewWAL(walConfig, customLogger)
	if err != nil {
		t.Fatalf("Failed to create new WAL: %v", err)
	}

	// Восстанавливаем данные из WAL
	logs, err := newWAL.Recover()
	if err != nil {
		t.Fatalf("Failed to restore from WAL: %v", err)
	}

	// Проверяем, что все записи восстановлены
	if len(logs) != 10 {
		t.Errorf("Expected 10 records to be recovered, got %d", len(logs))
	}

	// Проверяем, что все записи присутствуют
	found := make(map[string]bool)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		found[key] = false
	}

	for _, log := range logs {
		if log.Operation == "SET" && len(log.Args) >= 1 {
			found[log.Args[0]] = true
		}
	}

	for k, v := range found {
		if !v {
			t.Errorf("Expected key %s not found in recovered logs", k)
		}
	}

	// Закрываем новый WAL
	if err := newWAL.Close(); err != nil {
		t.Fatalf("Failed to close new WAL: %v", err)
	}
}
