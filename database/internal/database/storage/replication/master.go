package replication

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/keij-sama/Concurrency/database/internal/network"
	"github.com/keij-sama/Concurrency/pkg/logger"
	"go.uber.org/zap"
)

// Master представляет ведущий узел репликации
type Master struct {
	server       *network.TCPServer
	walDirectory string
	logger       logger.Logger
	ctx          context.Context
	cancel       context.CancelFunc
}

// NewMaster создает новый экземпляр Master
func NewMaster(server *network.TCPServer, walDirectory string, logger logger.Logger) (*Master, error) {
	if server == nil {
		return nil, errors.New("server is invalid")
	}

	// Создаем свой контекст, который будет отменен при закрытии мастера
	ctx, cancel := context.WithCancel(context.Background())

	return &Master{
		server:       server,
		walDirectory: walDirectory,
		logger:       logger,
		ctx:          ctx,
		cancel:       cancel,
	}, nil
}

// Start запускает обработку запросов репликации
func (m *Master) Start(ctx context.Context) error {
	m.logger.Info("Starting replication master",
		zap.String("wal_directory", m.walDirectory))

	// Обработчик запросов от слейвов
	handler := func(ctx context.Context, requestData []byte) []byte {
		// Проверяем контекст
		select {
		case <-ctx.Done():
			// Контекст отменен, возвращаем пустой ответ
			return nil
		default:
			// Продолжаем выполнение
		}

		var request Request
		if err := Decode(&request, requestData); err != nil {
			m.logger.Error("Failed to decode replication request", zap.Error(err))
			return encodeErrorResponse(errors.New("invalid request format"))
		}

		m.logger.Info("Received replication request",
			zap.String("last_segment", request.LastSegmentName))

		response := m.synchronize(request)
		responseData, err := Encode(response)
		if err != nil {
			m.logger.Error("Failed to encode replication response", zap.Error(err))
			return encodeErrorResponse(errors.New("failed to encode response"))
		}

		return responseData
	}

	// Запускаем обработку запросов с контекстом мастера, а не с переданным контекстом
	go m.server.HandleQueries(m.ctx, handler)
	return nil
}

// IsMaster возвращает true для Master
func (m *Master) IsMaster() bool {
	return true
}

// Close закрывает Master
func (m *Master) Close() error {
	m.logger.Info("Closing replication master")

	// Отменяем контекст, что остановит обработку запросов
	m.cancel()

	return nil
}

// synchronize обрабатывает запрос репликации
func (m *Master) synchronize(request Request) *Response {
	response := &Response{
		Succeed: false,
	}

	// Получаем следующий сегмент после lastSegmentName
	segmentName, err := findNextSegment(m.walDirectory, request.LastSegmentName)
	if err != nil {
		m.logger.Error("Failed to find next WAL segment",
			zap.String("last_segment", request.LastSegmentName),
			zap.Error(err))
		return response
	}

	if segmentName == "" {
		// Нет новых сегментов, все актуально
		response.Succeed = true
		m.logger.Info("No new WAL segments to send")
		return response
	}

	// Читаем данные сегмента
	segmentPath := filepath.Join(m.walDirectory, segmentName)
	data, err := os.ReadFile(segmentPath)
	if err != nil {
		m.logger.Error("Failed to read WAL segment",
			zap.String("segment", segmentName),
			zap.Error(err))
		return response
	}

	m.logger.Info("Sending WAL segment to slave",
		zap.String("segment", segmentName),
		zap.Int("size", len(data)))

	response.Succeed = true
	response.SegmentName = segmentName
	response.SegmentData = data
	return response
}

// findNextSegment находит следующий сегмент WAL после lastSegmentName
func findNextSegment(directory string, lastSegmentName string) (string, error) {
	segments, err := listWALSegments(directory)
	if err != nil {
		return "", err
	}

	if len(segments) == 0 {
		return "", nil
	}

	if lastSegmentName == "" {
		// Если это первый запрос, возвращаем первый сегмент
		return segments[0], nil
	}

	// Ищем следующий сегмент после lastSegmentName
	for i, segment := range segments {
		if segment == lastSegmentName && i < len(segments)-1 {
			return segments[i+1], nil
		}
	}

	// Если lastSegmentName не найден, возвращаем первый сегмент
	if !contains(segments, lastSegmentName) {
		return segments[0], nil
	}

	// Все сегменты уже получены
	return "", nil
}

// listWALSegments возвращает отсортированный список всех сегментов WAL
func listWALSegments(directory string) ([]string, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}

	var segments []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "wal_") &&
			strings.HasSuffix(entry.Name(), ".log") {
			segments = append(segments, entry.Name())
		}
	}

	// Сортируем сегменты по имени
	sort.Strings(segments)
	return segments, nil
}

// contains проверяет, содержит ли срез значение
func contains(slice []string, value string) bool {
	for _, item := range slice {
		if item == value {
			return true
		}
	}
	return false
}

// encodeErrorResponse кодирует ответ с ошибкой
func encodeErrorResponse(err error) []byte {
	response := &Response{
		Succeed: false,
		Error:   err.Error(),
	}
	data, _ := Encode(response)
	return data
}
