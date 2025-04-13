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
	ctx          context.Context
	cancel       context.CancelFunc
	done         chan struct{} // Канал для сигнализации о завершении
}

// NewSlave создает новый экземпляр Slave
func NewSlave(client *network.TCPClient, walDirectory string, syncInterval time.Duration,
	logger logger.Logger, walRecovery func([]wal.Log) error) (*Slave, error) {

	if client == nil {
		return nil, errors.New("client is invalid")
	}

	// Создаем свой контекст, который будет отменен при закрытии слейва
	ctx, cancel := context.WithCancel(context.Background())

	return &Slave{
		client:       client,
		walDirectory: walDirectory,
		syncInterval: syncInterval,
		logger:       logger,
		walRecovery:  walRecovery,
		ctx:          ctx,
		cancel:       cancel,
		done:         make(chan struct{}),
	}, nil
}

// Start запускает процесс синхронизации с мастером
func (s *Slave) Start(ctx context.Context) error {
	s.logger.Info("Starting replication slave",
		zap.String("wal_directory", s.walDirectory),
		zap.Duration("sync_interval", s.syncInterval))

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
	go s.syncLoop()

	return nil
}

// IsMaster возвращает false для Slave
func (s *Slave) IsMaster() bool {
	return false
}

// Close закрывает Slave
func (s *Slave) Close() error {
	s.logger.Info("Closing replication slave")

	// Отменяем контекст, что остановит syncLoop
	s.cancel()

	// Ждем завершения syncLoop
	select {
	case <-s.done:
		s.logger.Info("Slave sync loop stopped gracefully")
	case <-time.After(time.Second):
		s.logger.Info("Timeout waiting for slave sync loop to stop")
	}

	// Закрываем клиент
	s.client.Close()

	return nil
}

// syncLoop периодически синхронизируется с мастером
func (s *Slave) syncLoop() {
	defer close(s.done) // Сигнализируем о завершении при выходе

	ticker := time.NewTicker(s.syncInterval)
	defer ticker.Stop()

	s.logger.Info("Starting sync loop")

	// Выполняем первую синхронизацию немедленно
	if err := s.sync(); err != nil {
		s.logger.Error("Initial sync failed", zap.Error(err))
	}

	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("Sync loop terminated due to context cancellation")
			return
		case <-ticker.C:
			if err := s.sync(); err != nil {
				s.logger.Error("Sync failed", zap.Error(err))
				// Продолжаем работу даже при ошибках
			}
		}
	}
}

// sync выполняет одну синхронизацию с мастером
func (s *Slave) sync() error {
	// Проверка контекста на завершение
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
		// Продолжаем выполнение
	}

	s.logger.Info("Starting sync with master",
		zap.String("last_segment", s.lastSegment))

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
		return fmt.Errorf("master reported sync failure: %s", response.Error)
	}

	// Если мастер не вернул новый сегмент, все в порядке
	if response.SegmentName == "" {
		s.logger.Info("No new WAL segments from master")
		return nil
	}

	s.logger.Info("Received WAL segment from master",
		zap.String("segment", response.SegmentName),
		zap.Int("size", len(response.SegmentData)))

	// Сохраняем полученный сегмент на диск
	segmentPath := filepath.Join(s.walDirectory, response.SegmentName)
	if err := os.WriteFile(segmentPath, response.SegmentData, 0644); err != nil {
		return fmt.Errorf("failed to write WAL segment: %w", err)
	}

	// Обновляем последний полученный сегмент
	s.lastSegment = response.SegmentName

	// Применяем изменения из WAL
	if err := s.applyWALSegment(segmentPath); err != nil {
		return fmt.Errorf("failed to apply WAL segment: %w", err)
	}

	s.logger.Info("Successfully applied WAL segment",
		zap.String("segment", response.SegmentName))

	return nil
}

// applyWALSegment применяет изменения из сегмента WAL
func (s *Slave) applyWALSegment(segmentPath string) error {
	// Читаем записи WAL из сегмента
	logs, err := wal.ReadLogsFromFile(segmentPath)
	if err != nil {
		return fmt.Errorf("failed to read logs from WAL segment: %w", err)
	}

	s.logger.Info("Applying WAL segment",
		zap.String("path", segmentPath),
		zap.Int("logs_count", len(logs)))

	// Применяем изменения
	if s.walRecovery != nil {
		return s.walRecovery(logs)
	}

	return nil
}
