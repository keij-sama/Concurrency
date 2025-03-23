package network

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Обработка запросов
type TCPHandler func(context.Context, []byte) []byte

// Сервер базы данных
type TCPServer struct {
	listener       net.Listener
	idleTimeout    time.Duration
	bufferSize     int
	maxConnections int
	logger         *zap.Logger
	activeConns    chan struct{} // Канал для ограничения количества соединений
}

// Опция для конфигурации сервера
type TCPServerOption func(*TCPServer)

// Устанавливает максимальное количество соединений
func WithMaxConnections(maxConnections int) TCPServerOption {
	return func(s *TCPServer) {
		s.maxConnections = maxConnections
	}
}

// Устанавливает таймаут неактивности
func WithIdleTimeout(timeout time.Duration) TCPServerOption {
	return func(s *TCPServer) {
		s.idleTimeout = timeout
	}
}

// Устанавливает размер буфера для чтения
func WithBufferSize(size int) TCPServerOption {
	return func(s *TCPServer) {
		s.bufferSize = size
	}
}

// создает новый TCP сервер
func NewTCPServer(address string, logger *zap.Logger, options ...TCPServerOption) (*TCPServer, error) {
	if logger == nil {
		return nil, errors.New("logger is invalid")
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}

	server := &TCPServer{
		listener: listener,
		logger:   logger,
	}

	for _, option := range options {
		option(server)
	}

	// Устанавливаем значения по умолчанию, если не указаны
	if server.maxConnections <= 0 {
		server.maxConnections = 100 // по умолчанию 100 соединений
	}

	if server.bufferSize <= 0 {
		server.bufferSize = 4 << 10 // по умолчанию 4
	}

	// Создаем канал для ограничения соединений
	server.activeConns = make(chan struct{}, server.maxConnections)

	return server, nil
}

func (s *TCPServer) HandleQueries(ctx context.Context, handler TCPHandler) {
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			// Проверяем сигнал завершения
			select {
			case <-ctx.Done():
				return
			default:
				// Продолжаем принимать соединения
			}

			// Новое соединение
			connection, err := s.listener.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				s.logger.Error("failed to accept", zap.Error(err))
				continue
			}

			// Проверяем, можем ли принять соединение
			select {
			case s.activeConns <- struct{}{}: // Занимаем место
				// Обрабатываем соединение в новой горутине
				wg.Add(1)
				go func(connection net.Conn) {
					defer wg.Done()
					defer func() { <-s.activeConns }() // Освобождаем место
					s.handleConnection(ctx, connection, handler)
				}(connection)
			default:
				// Если достигнут лимит, закрываем соединение
				s.logger.Warn("connection limit reached, rejecting connection")
				connection.Close()
			}
		}
	}()
	<-ctx.Done()
	s.listener.Close()
	wg.Wait()
}

// обрабатывает соединение с клиентом
func (s *TCPServer) handleConnection(ctx context.Context, connection net.Conn, handler TCPHandler) {
	defer func() {
		if v := recover(); v != nil {
			s.logger.Error("captured panic", zap.Any("panic", v))
		}
		if err := connection.Close(); err != nil {
			s.logger.Warn("failed to close connection", zap.Error(err))
		}
	}()

	// Буфер для запросов
	request := make([]byte, s.bufferSize)

	for {
		// Проверяем контекст
		select {
		case <-ctx.Done():
			return
		default:
			//Продолжаем обработку
		}

		// Устанавливаем таймаут чтения, если указан
		if s.idleTimeout > 0 {
			if err := connection.SetReadDeadline(time.Now().Add(s.idleTimeout)); err != nil {
				s.logger.Warn("failed to set read deadline", zap.Error(err))
				break
			}
		}

		// Читаем запрос
		count, err := connection.Read(request)
		if err != nil {
			if err != io.EOF {
				s.logger.Warn(
					"failed to read data",
					zap.String("address", connection.RemoteAddr().String()),
					zap.Error(err),
				)
			}
			break
		} else if count == s.bufferSize {
			s.logger.Warn("buffer size may be too small", zap.Int("buffer_size", s.bufferSize))
			break
		}

		// Устанавливаем таймаут записи, если указан
		if s.idleTimeout > 0 {
			if err := connection.SetWriteDeadline(time.Now().Add(s.idleTimeout)); err != nil {
				s.logger.Warn("failed to set write deadline", zap.Error(err))
				break
			}
		}

		// Обрабатываем запрос
		response := handler(ctx, request[:count])

		// Отправляем ответ
		if _, err := connection.Write(response); err != nil {
			s.logger.Warn(
				"failed to write data",
				zap.String("address", connection.RemoteAddr().String()),
				zap.Error(err),
			)
			break
		}
	}
}
