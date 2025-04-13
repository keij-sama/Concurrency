package config

import (
	"fmt"
	"io/ioutil"
	"time"

	"github.com/keij-sama/Concurrency/database/internal/database/storage/replication"
	"github.com/keij-sama/Concurrency/database/internal/database/storage/wal"
	"gopkg.in/yaml.v3"
)

// Config представляет полную конфигурацию базы данных
type Config struct {
	Engine      EngineConfig      `yaml:"engine"`
	Network     NetworkConfig     `yaml:"network"`
	Logging     LoggingConfig     `yaml:"logging"`
	WAL         WALConfig         `yaml:"wal"`
	Replication ReplicationConfig `yaml:"replication"`
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

type WALConfig struct {
	Enabled              bool   `yaml:"enabled"`
	FlushingBatchSize    int    `yaml:"flushing_batch_size"`
	FlushingBatchTimeout string `yaml:"flushing_batch_timeout"`
	MaxSegmentSize       string `yaml:"max_segment_size"`
	DataDirectory        string `yaml:"data_directory"`
}

// ReplicationConfig представляет конфигурацию репликации
type ReplicationConfig struct {
	Enabled       bool   `yaml:"enabled"`        // Включена ли репликация
	ReplicaType   string `yaml:"replica_type"`   // Тип реплики (master/slave)
	MasterAddress string `yaml:"master_address"` // Адрес мастера (для slave)
	SyncInterval  string `yaml:"sync_interval"`  // Интервал синхронизации
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
		WAL: WALConfig{
			Enabled:              false,
			FlushingBatchSize:    100,
			FlushingBatchTimeout: "10ms",
			MaxSegmentSize:       "10MB",
			DataDirectory:        "/data/spider/wal",
		},
		Replication: ReplicationConfig{
			Enabled:       false,
			ReplicaType:   "master",
			MasterAddress: "127.0.0.1:3232",
			SyncInterval:  "1s",
		},
	}
}

// GetWALConfig конвертирует конфигурацию WAL из YAML в объект wal.WALConfig
func (c *Config) GetWALConfig() *wal.WALConfig {
	if !c.WAL.Enabled {
		return nil
	}

	// Парсим параметры
	var flushTimeout time.Duration
	if c.WAL.FlushingBatchTimeout != "" {
		flushTimeout, _ = time.ParseDuration(c.WAL.FlushingBatchTimeout)
	} else {
		flushTimeout = 10 * time.Millisecond
	}

	var maxSegmentSize int64
	if c.WAL.MaxSegmentSize != "" {
		fmt.Sscanf(c.WAL.MaxSegmentSize, "%dMB", &maxSegmentSize)
		maxSegmentSize = maxSegmentSize * 1024 * 1024 // Конвертируем MB в байты
	} else {
		maxSegmentSize = 10 * 1024 * 1024 // 10MB по умолчанию
	}

	return &wal.WALConfig{
		Enabled:              c.WAL.Enabled,
		FlushingBatchSize:    c.WAL.FlushingBatchSize,
		FlushingBatchTimeout: flushTimeout,
		MaxSegmentSize:       maxSegmentSize,
		DataDirectory:        c.WAL.DataDirectory,
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

// GetReplicationConfig преобразует конфигурацию репликации
func (c *Config) GetReplicationConfig() *replication.ReplicationConfig {
	if !c.Replication.Enabled {
		return nil
	}

	// Парсим интервал синхронизации
	syncInterval, err := time.ParseDuration(c.Replication.SyncInterval)
	if err != nil {
		syncInterval = 1 * time.Second // По умолчанию 1 секунда
	}

	var replicaType replication.ReplicationType
	switch c.Replication.ReplicaType {
	case "slave":
		replicaType = replication.TypeSlave
	default:
		replicaType = replication.TypeMaster
	}

	return &replication.ReplicationConfig{
		Enabled:       c.Replication.Enabled,
		ReplicaType:   replicaType,
		MasterAddress: c.Replication.MasterAddress,
		SyncInterval:  syncInterval,
	}
}
