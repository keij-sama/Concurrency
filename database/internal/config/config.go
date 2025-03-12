package config

import (
	"io/ioutil"
	"time"

	"gopkg.in/yaml.v3"
)

// Config представляет полную конфигурацию базы данных
type Config struct {
	Engine  EngineConfig  `yaml:"engine"`
	Network NetworkConfig `yaml:"network"`
	Logging LoggingConfig `yaml:"logging"`
}

// EngineConfig представляет конфигурацию движка базы данных
type EngineConfig struct {
	Type string `yaml:"type"`
}

// NetworkConfig представляет конфигурацию сети
type NetworkConfig struct {
	Address        string        `yaml:"address"`
	MaxConnections int           `yaml:"max_connections"`
	MaxMessageSize string        `yaml:"max_message_size"`
	IdleTimeout    time.Duration `yaml:"idle_timeout"`
}

// LoggingConfig представляет конфигурацию логирования
type LoggingConfig struct {
	Level  string `yaml:"level"`
	Output string `yaml:"uotput"`
}

func DefaultConfig() *Config {
	return &Config{
		Engine: EngineConfig{
			Type: "in_memory",
		},
		Network: NetworkConfig{
			Address:        "127.0.0.1:3223",
			MaxConnections: 100,
			MaxMessageSize: "4KB",
			IdleTimeout:    5 * time.Minute,
		},
		Logging: LoggingConfig{
			Level:  "info",
			Output: "stdout",
		},
	}
}

// LoadConfig загружает конфигурацию из YAML-файла
func LoadConfig(filename string) (*Config, error) {
	// Начинаем с конфигурации по умолчанию
	config := DefaultConfig()

	// Читаем файл конфигурации
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return config, err // Возвращаем конфигурацию по умолчанию, если файл не найден
	}

	// Разбираем YAML
	err = yaml.Unmarshal(data, config)
	if err != nil {
		return config, err
	}
	return config, nil
}
