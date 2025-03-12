package config

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	// Проверяем значения по умолчанию
	if cfg.Engine.Type != "in_memory" {
		t.Errorf("Default engine type should be 'in_memory', got %s", cfg.Engine.Type)
	}

	if cfg.Network.Address != "127.0.0.1:3223" {
		t.Errorf("Default address should be '127.0.0.1:3223', got %s", cfg.Network.Address)
	}

	if cfg.Network.MaxConnections != 100 {
		t.Errorf("Default max connections should be 100, got %d", cfg.Network.MaxConnections)
	}

	if cfg.Logging.Level != "info" {
		t.Errorf("Default logging level should be 'info', got %s", cfg.Logging.Level)
	}
}

func TestLoadConfig(t *testing.T) {
	// Создаем временный файл конфигурации
	content := `
engine:
  type: "in_memory"
network:
  address: "127.0.0.1:9999"
  max_connections: 50
  max_message_size: "2KB"
  idle_timeout: 10m
logging:
  level: "debug"
  output: "/tmp/test.log"
`

	tmpfile, err := ioutil.TempFile("", "config.*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(content)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatalf("Failed to close temp file: %v", err)
	}

	// Загружаем конфигурацию
	cfg, err := LoadConfig(tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Проверяем значения из файла
	if cfg.Network.Address != "127.0.0.1:9999" {
		t.Errorf("Address should be '127.0.0.1:9999', got %s", cfg.Network.Address)
	}

	if cfg.Network.MaxConnections != 50 {
		t.Errorf("Max connections should be 50, got %d", cfg.Network.MaxConnections)
	}

	if cfg.Network.IdleTimeout != 10*time.Minute {
		t.Errorf("Idle timeout should be 10m, got %s", cfg.Network.IdleTimeout)
	}

	if cfg.Logging.Level != "debug" {
		t.Errorf("Logging level should be 'debug', got %s", cfg.Logging.Level)
	}
}

func TestMissingConfigFile(t *testing.T) {
	// Пытаемся загрузить несуществующий файл
	cfg, err := LoadConfig("non_existent_file.yaml")

	// Должны получить ошибку, но также и значения по умолчанию
	if err == nil {
		t.Errorf("Expected error for non-existent file")
	}

	// Проверяем, что значения по умолчанию применены
	if cfg.Engine.Type != "in_memory" {
		t.Errorf("Should use default values when file not found")
	}
}
