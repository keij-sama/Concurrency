package replication

import (
	"context"
	"encoding/json"
	"time"
)

// ReplicationType определяет тип репликации
type ReplicationType string

const (
	// TypeMaster - ведущий узел
	TypeMaster ReplicationType = "master"
	// TypeSlave - ведомый узел
	TypeSlave ReplicationType = "slave"
)

// ReplicationConfig содержит настройки репликации
type ReplicationConfig struct {
	Enabled       bool            `yaml:"enabled"`        // Включена ли репликация
	ReplicaType   ReplicationType `yaml:"replica_type"`   // Тип реплики (master/slave)
	MasterAddress string          `yaml:"master_address"` // Адрес мастера для подключения
	SyncInterval  time.Duration   `yaml:"sync_interval"`  // Интервал синхронизации
}

// Replication определяет интерфейс для репликации
type Replication interface {
	Start(ctx context.Context) error
	IsMaster() bool
	Close() error
}

// Request представляет запрос от slave к master
type Request struct {
	LastSegmentName string `json:"last_segment_name"` // Имя последнего полученного сегмента
}

// Response представляет ответ от master к slave
type Response struct {
	Succeed     bool   `json:"succeed"`      // Успешность операции
	Error       string `json:"error"`        // Сообщение об ошибке (если есть)
	SegmentName string `json:"segment_name"` // Имя сегмента
	SegmentData []byte `json:"segment_data"` // Данные сегмента
}

// Encode кодирует объект в JSON
func Encode(obj interface{}) ([]byte, error) {
	return json.Marshal(obj)
}

// Decode декодирует JSON в объект
func Decode(obj interface{}, data []byte) error {
	return json.Unmarshal(data, obj)
}
