package engine

import (
	"errors"
	"sync"
)

// Константы
const (
	// Количество партиций в хеш-таблице
	numPartitions = 16
)

// Errors
var (
	ErrKeyNotFound = errors.New("key not found")
)

// Engine определяет интерфейс для хранения и получения пар ключ-значение
type Engine interface {
	Set(key, value string) error
	Get(key string) (string, error)
	Delete(key string) error
}

// Partition представляет одну партицию хеш-таблицы
type Partition struct {
	data map[string]string
	mu   sync.RWMutex
}

// InMemoryEngine реализует in-memory движок с партицированием
type InMemoryEngine struct {
	partitions [numPartitions]Partition
}

// NewInMemoryEngine создает новый in-memory движок
func NewInMemoryEngine() Engine {
	engine := &InMemoryEngine{}

	// Инициализируем партиции
	for i := 0; i < numPartitions; i++ {
		engine.partitions[i].data = make(map[string]string)
	}

	return engine
}

// getPartition возвращает номер партиции для ключа
func getPartition(key string) int {
	// Простая хеш-функция для определения партиции
	hash := 0
	for _, c := range key {
		hash = hash*31 + int(c)
	}

	// Берем абсолютное значение и приводим к диапазону партиций
	if hash < 0 {
		hash = -hash
	}
	return hash % numPartitions
}

// Set сохраняет пару ключ-значение
func (e *InMemoryEngine) Set(key, value string) error {
	// Определяем партицию
	partIdx := getPartition(key)
	partition := &e.partitions[partIdx]

	// Блокируем только нужную партицию для записи
	partition.mu.Lock()
	defer partition.mu.Unlock()

	partition.data[key] = value
	return nil
}

// Get получает значение по ключу
func (e *InMemoryEngine) Get(key string) (string, error) {
	// Определяем партицию
	partIdx := getPartition(key)
	partition := &e.partitions[partIdx]

	// Блокируем только нужную партицию для чтения
	partition.mu.RLock()
	defer partition.mu.RUnlock()

	value, exists := partition.data[key]
	if !exists {
		return "", ErrKeyNotFound
	}

	return value, nil
}

// Delete удаляет пару ключ-значение
func (e *InMemoryEngine) Delete(key string) error {
	// Определяем партицию
	partIdx := getPartition(key)
	partition := &e.partitions[partIdx]

	// Блокируем только нужную партицию для записи
	partition.mu.Lock()
	defer partition.mu.Unlock()

	if _, exists := partition.data[key]; !exists {
		return ErrKeyNotFound
	}

	delete(partition.data, key)
	return nil
}
