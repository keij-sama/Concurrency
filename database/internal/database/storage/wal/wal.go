package wal

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/keij-sama/Concurrency/pkg/logger"
	"go.uber.org/zap"
)

const (
	OperationSet = "SET"
	OperationDel = "DEL"
)

// LogRecord представляет запись в WAL
type Log struct {
	LSN       uint64   `json:"lsn"`
	Operation string   `json:"operation"`
	Args      []string `json:"args"`
}

// Представляет запрос в WAL
type WriteRequest struct {
	Log  Log
	Done chan error
}

// NewWriteRequest создает новый запрос на запись
func NewWriteRequest(operation string, args []string) WriteRequest {
	return WriteRequest{
		Log: Log{
			Operation: operation,
			Args:      args,
		},
		Done: make(chan error, 1),
	}
}

// Возвращает канал для получения результата операций
func (w WriteRequest) FutureResponse() chan error {
	return w.Done
}

// Содержит настройки WAL
type WALConfig struct {
	Enabled              bool          // включен ли wal
	FlushingBatchSize    int           // размер пакета для записи
	FlushingBatchTimeout time.Duration // таймаут записи
	MaxSegmentSize       int64         // максимальный размер сегмента в байтах
	DataDirectory        string        // директория для хранения wal
}

type WAL struct {
	config       WALConfig
	logger       logger.Logger
	currentFile  *os.File
	currentSize  int64
	nextLSN      uint64
	segments     []string
	mutex        sync.Mutex
	batch        []WriteRequest
	batches      chan []WriteRequest
	segmentMutex sync.Mutex
}

// NewWAL создает новый экземпляр WAL
func NewWAL(config WALConfig, logger logger.Logger) (*WAL, error) {
	// Проверка, включен ли WAL
	if !config.Enabled {
		return nil, nil
	}

	// Проверяем настройки WAL
	if config.FlushingBatchSize <= 0 {
		config.FlushingBatchSize = 100
	}
	if config.FlushingBatchTimeout <= 0 {
		config.FlushingBatchTimeout = 10 * time.Millisecond
	}
	if config.MaxSegmentSize <= 0 {
		config.MaxSegmentSize = 10 * 1024 * 1024 // 10MB по умолчанию
	}

	// Создаем директорию для WAL
	if err := os.MkdirAll(config.DataDirectory, 0755); err != nil {
		return nil, fmt.Errorf("не удалось создать директорию WAL: %w", err)
	}

	// Ищем существующие сегменты
	segments, err := filepath.Glob(filepath.Join(config.DataDirectory, "wal_*.log"))
	if err != nil {
		return nil, fmt.Errorf("не удалось найти сегменты WAL: %w", err)
	}

	// Создаем или открываем текущий файл сегмента
	var currentFile *os.File
	var nextLSN uint64 = 0

	if len(segments) > 0 {
		// Если есть существующие сегменты, восстанавливаем последний LSN
		logs, err := readLogs(segments)
		if err != nil {
			return nil, fmt.Errorf("не удалось прочитать логи: %w", err)
		}

		for _, log := range logs {
			if log.LSN >= nextLSN {
				nextLSN = log.LSN + 1
			}
		}

		// Открываем новый сегмент
		currentFile, err = os.OpenFile(
			filepath.Join(config.DataDirectory, fmt.Sprintf("wal_%d.log", len(segments))),
			os.O_CREATE|os.O_APPEND|os.O_WRONLY,
			0644,
		)
		if err != nil {
			return nil, fmt.Errorf("не удалось создать новый сегмент WAL: %w", err)
		}
	} else {
		// Создаем первый сегмент
		currentFile, err = os.OpenFile(
			filepath.Join(config.DataDirectory, "wal_0.log"),
			os.O_CREATE|os.O_APPEND|os.O_WRONLY,
			0644,
		)
		if err != nil {
			return nil, fmt.Errorf("не удалось создать первый сегмент WAL: %w", err)
		}
		segments = []string{filepath.Join(config.DataDirectory, "wal_0.log")}
	}

	// Получаем текущий размер файла
	info, err := currentFile.Stat()
	if err != nil {
		currentFile.Close()
		return nil, fmt.Errorf("не удалось получить информацию о файле: %w", err)
	}

	return &WAL{
		config:      config,
		logger:      logger,
		currentFile: currentFile,
		currentSize: info.Size(),
		nextLSN:     nextLSN,
		segments:    segments,
		batches:     make(chan []WriteRequest, 1),
	}, nil
}

// Start запускает процесс WAL
func (w *WAL) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(w.config.FlushingBatchTimeout)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				w.flushBatch()
				return
			default:
			}

			select {
			case <-ctx.Done():
				w.flushBatch()
				return
			case batch := <-w.batches:
				w.writeBatch(batch)
				ticker.Reset(w.config.FlushingBatchTimeout)
			case <-ticker.C:
				w.flushBatch()
			}
		}
	}()
}

// Recover восстанавливает данные из WAL
func (w *WAL) Recover() ([]Log, error) {
	return readLogs(w.segments)
}

// Set записывает операцию SET в WAL
func (w *WAL) Set(key, value string) chan error {
	return w.push("SET", []string{key, value})
}

// Del записывает операцию DEL в WAL
func (w *WAL) Del(key string) chan error {
	return w.push("DEL", []string{key})
}

// push добавляет операцию в батч
func (w *WAL) push(operation string, args []string) chan error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Создаем запрос на запись
	req := NewWriteRequest(operation, args)

	// Устанавливаем LSN
	req.Log.LSN = w.nextLSN
	w.nextLSN++

	// Добавляем в батч
	w.batch = append(w.batch, req)

	// Если батч достиг максимального размера, отправляем его на запись
	if len(w.batch) >= w.config.FlushingBatchSize {
		w.batches <- w.batch
		w.batch = nil
	}

	return req.Done
}

// flushBatch записывает текущий батч на диск
func (w *WAL) flushBatch() {
	var batch []WriteRequest

	w.mutex.Lock()
	batch = w.batch
	w.batch = nil
	w.mutex.Unlock()

	if len(batch) > 0 {
		w.writeBatch(batch)
	}
}

// writeBatch записывает батч в файл
func (w *WAL) writeBatch(batch []WriteRequest) {
	if len(batch) == 0 {
		return
	}

	// Извлекаем логи из запросов
	logs := make([]Log, len(batch))
	for i, req := range batch {
		logs[i] = req.Log
	}

	// Сериализуем логи в JSON
	data, err := json.Marshal(logs)
	if err != nil {
		w.logger.Error("Не удалось сериализовать логи", zap.Error(err))
		completeAllWithError(batch, err)
		return
	}

	// Проверяем, нужно ли создать новый сегмент
	w.segmentMutex.Lock()
	if w.currentSize+int64(len(data)+1) > w.config.MaxSegmentSize {
		// Закрываем текущий файл
		w.currentFile.Close()

		// Создаем новый сегмент
		newFile, err := os.OpenFile(
			filepath.Join(w.config.DataDirectory, fmt.Sprintf("wal_%d.log", len(w.segments))),
			os.O_CREATE|os.O_APPEND|os.O_WRONLY,
			0644,
		)
		if err != nil {
			w.logger.Error("Не удалось создать новый сегмент WAL", zap.Error(err))
			w.segmentMutex.Unlock()
			completeAllWithError(batch, err)
			return
		}

		w.currentFile = newFile
		w.currentSize = 0
		w.segments = append(w.segments, newFile.Name())
	}
	w.segmentMutex.Unlock()

	// Записываем данные с переводом строки в конце
	data = append(data, '\n')
	n, err := w.currentFile.Write(data)
	if err != nil {
		w.logger.Error("Не удалось записать данные в WAL", zap.Error(err))
		completeAllWithError(batch, err)
		return
	}

	// Синхронизируем с диском
	if err := w.currentFile.Sync(); err != nil {
		w.logger.Error("Не удалось синхронизировать WAL с диском", zap.Error(err))
		completeAllWithError(batch, err)
		return
	}

	// Обновляем размер файла
	w.currentSize += int64(n)

	// Уведомляем о завершении операций
	completeAllWithSuccess(batch)
}

// Close закрывает WAL
func (w *WAL) Close() error {
	// Записываем оставшиеся данные
	w.flushBatch()

	// Закрываем файл
	if w.currentFile != nil {
		return w.currentFile.Close()
	}
	return nil
}

// readLogs читает логи из сегментов
func readLogs(segments []string) ([]Log, error) {
	var allLogs []Log

	for _, segment := range segments {
		// Открываем файл сегмента
		file, err := os.Open(segment)
		if err != nil {
			return nil, fmt.Errorf("не удалось открыть сегмент WAL: %w", err)
		}
		defer file.Close()

		// Читаем файл построчно
		decoder := json.NewDecoder(file)
		for {
			var logs []Log
			if err := decoder.Decode(&logs); err != nil {
				if err.Error() == "EOF" {
					break
				}
				return nil, fmt.Errorf("не удалось декодировать логи: %w", err)
			}
			allLogs = append(allLogs, logs...)
		}
	}

	return allLogs, nil
}

// completeAllWithError уведомляет о завершении всех запросов с ошибкой
func completeAllWithError(batch []WriteRequest, err error) {
	for _, req := range batch {
		req.Done <- err
		close(req.Done)
	}
}

// completeAllWithSuccess уведомляет о успешном завершении всех запросов
func completeAllWithSuccess(batch []WriteRequest) {
	for _, req := range batch {
		req.Done <- nil
		close(req.Done)
	}
}
