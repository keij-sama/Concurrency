package engine

import (
	"errors"
)

// Ошибки
var (
	ErrKeyNotFound = errors.New("key not found")
)

// Engine определяет интерфейс для хранения и получения пар ключ-значение
type Engine interface {
	Set(key, value string) error
	Get(key string) (string, error)
	Delete(key string) error
}

// InMemoryEngine - это реализация интерфейса Engine в памяти
type InMemoryEngine struct {
	data map[string]string
	// Место для будущей реализации concurrency
}

// NewInMemoryEngine создает новый движок in-memory
func NewInMemoryEngine() Engine {
	return &InMemoryEngine{
		data: make(map[string]string),
	}
}

// Set хранит пару ключ-значение
func (e *InMemoryEngine) Set(key, value string) error {
	e.data[key] = value
	return nil
}

// Get получает значение по ключу
func (e *InMemoryEngine) Get(key string) (string, error) {
	value, exists := e.data[key]
	if !exists {
		return "", ErrKeyNotFound
	}
	return value, nil
}

// Delete удаляет пару ключ-значение
func (e *InMemoryEngine) Delete(key string) error {
	if _, exists := e.data[key]; !exists {
		return ErrKeyNotFound
	}
	delete(e.data, key)
	return nil
}
