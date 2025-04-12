package replication

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/keij-sama/Concurrency/database/internal/database/storage/wal"
	"github.com/keij-sama/Concurrency/database/internal/network"
	"github.com/keij-sama/Concurrency/pkg/logger"
	"go.uber.org/zap"
)

// Slave представляет ведомый узел репликации
type Slave struct {
	client       *network.TCPClient
	walDirectory string
	syncInterval time.Duration
	logger       logger.Logger
	lastSegment  string
	walRecovery  func([]wal.Log) error // Функция для восстановления из WAL
}

// NewSlave создает новый экземпляр Slave
func NewSlave(client *network.TCPClient, walDirectory string, syncInterval time.Duration,
	logger logger.Logger, walRecovery func([]wal.Log) error) (*Slave, error) {

	if client == nil {
		return nil, errors.New("client is invalid")
	}

	return &Slave{
		client:       client,
		walDirectory: walDirectory,
		syncInterval: syncInterval,
		logger:       logger,
		walRecovery:  walRecovery,
	}, nil
}

// Start запускает процесс синхронизации с мастером
func (s *Slave) Start(ctx context.Context) error {
	s.logger.Info("Starting replication slave",
		zap.String("wal_directory", s.walDirectory))

	// Определяем последний полученный сегмент
	segments, err := listWALSegments(s.walDirectory)
	if err != nil {
		return fmt.Errorf("failed to list WAL segments: %w", err)
	}

	if len(segments) > 0 {
		s.lastSegment = segments[len(segments)-1]
		s.logger.Info("Found last WAL segment", zap.String("segment", s.lastSegment))
	}

	// Запускаем процесс синхронизации
	go s.syncLoop(ctx)

	return nil
}

// IsMaster возвращает false для Slave
func (s *Slave) IsMaster() bool {
	return false
}

// Close закрывает Slave
func (s *Slave) Close() error {
	return nil
}

// syncLoop периодически синхронизируется с мастером
func (s *Slave) syncLoop(ctx context.Context) {
	ticker := time.NewTicker(s.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.sync(ctx); err != nil {
				s.logger.Error("Sync failed", zap.Error(err))
			}
		}
	}
}

// sync выполняет одну синхронизацию с мастером
func (s *Slave) sync(ctx context.Context) error {
	request := Request{
		LastSegmentName: s.lastSegment,
	}

	requestData, err := Encode(request)
	if err != nil {
		return fmt.Errorf("failed to encode request: %w", err)
	}

	// Отправляем запрос мастеру
	responseData, err := s.client.Send(requestData)
	if err != nil {
		return fmt.Errorf("failed to send request to master: %w", err)
	}

	var response Response
	if err := Decode(&response, responseData); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if !response.Succeed {
		return fmt.Errorf("master reported sync failure")
	}

	// Если мастер не вернул новый сегмент, все в порядке
	if response.SegmentName == "" {
		s.logger.Debug("No new WAL segments from master")
		return nil
	}

	// Сохраняем полученный сегмент на диск
	segmentPath := filepath.Join(s.walDirectory, response.SegmentName)
	if err := os.WriteFile(segmentPath, response.SegmentData, 0644); err != nil {
		return fmt.Errorf("failed to write WAL segment: %w", err)
	}

	s.logger.Info("Received WAL segment from master",
		zap.String("segment", response.SegmentName),
		zap.Int("size", len(response.SegmentData)))

	// Обновляем последний полученный сегмент
	s.lastSegment = response.SegmentName

	// Применяем изменения из WAL
	if err := s.applyWALSegment(segmentPath); err != nil {
		return fmt.Errorf("failed to apply WAL segment: %w", err)
	}

	return nil
}

// applyWALSegment применяет изменения из сегмента WAL
func (s *Slave) applyWALSegment(segmentPath string) error {
	// Читаем записи WAL из сегмента
	logs, err := wal.ReadLogsFromFile(segmentPath)
	if err != nil {
		return err
	}

	// Применяем изменения
	if s.walRecovery != nil {
		return s.walRecovery(logs)
	}

	return nil
}
