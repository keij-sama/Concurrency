package engine

import "testing"

func TestInMemoryEngine(t *testing.T) {
	t.Run("Set and Get", func(t *testing.T) {
		e := NewInMemoryEngine()

		// Сохранение пары ключ-значение
		err := e.Set("test_key", "test_value")
		if err != nil {
			t.Errorf("Set() error: %v", err)
		}

		// Получение значения
		val, err := e.Get("test_key")
		if err != nil {
			t.Errorf("Get() error: %v", err)
		}

		if val != "test_value" {
			t.Errorf("Get() = %v, want %v", val, "test_value")
		}
	})

	t.Run("Delete an existing key", func(t *testing.T) {
		e := NewInMemoryEngine()

		// Сохранение пары ключ-значение
		e.Set("test_key", "test_value")

		// Удаление ключа
		err := e.Delete("test_key")
		if err != nil {
			t.Errorf("Delete() error: %v", err)
		}

		// Проверяем, что ключ удалён
		_, err = e.Get("test_key")
		if err != ErrKeyNotFound {
			t.Errorf("Get() after Delete() should return ErrKeyNotFound")
		}
	})

	t.Run("Delete non-existent key", func(t *testing.T) {
		e := NewInMemoryEngine()

		err := e.Delete("non-existen_key")
		if err != ErrKeyNotFound {
			t.Errorf("Delete() of a non-existent key should return ErrKeyNotFound")
		}
	})
}
