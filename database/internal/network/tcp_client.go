package network

import (
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

const defaultBufferSize = 4 << 10 // 4KB

// Представляет клиента для TCP-подключения к базе данных
type TCPClient struct {
	connection  net.Conn
	idleTimeout time.Duration
	bufferSize  int
}

// Опция для конфигурации клиента
type TCPClientOption func(*TCPClient)

// устанавливает таймаут неактивности для клиента
func WithClientIdleTimeout(timeout time.Duration) TCPClientOption {
	return func(c *TCPClient) {
		c.idleTimeout = timeout
	}
}

// устанавливает размер буфера для чтения клиента
func WithClientBufferSize(size int) TCPClientOption {
	return func(c *TCPClient) {
		c.bufferSize = size
	}
}

// создает нового TCP клиента
func NewTCPClient(address string, options ...TCPClientOption) (*TCPClient, error) {
	connection, err := net.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}

	client := &TCPClient{
		connection: connection,
		bufferSize: defaultBufferSize,
	}

	for _, option := range options {
		option(client)
	}

	if client.idleTimeout != 0 {
		if err := connection.SetDeadline(time.Now().Add(client.idleTimeout)); err != nil {
			return nil, fmt.Errorf("failed to set deadline for connection: %w", err)
		}
	}

	return client, nil
}

// Send отправляет запрос и получает ответ
func (c *TCPClient) Send(request []byte) ([]byte, error) {
	if _, err := c.connection.Write(request); err != nil {
		return nil, err
	}

	response := make([]byte, c.bufferSize)
	count, err := c.connection.Read(response)
	if err != nil && err != io.EOF {
		return nil, err
	} else if count == c.bufferSize {
		return nil, errors.New("small buffer size")
	}
	return response[:count], nil
}

// Close закрывает соединение
func (c *TCPClient) Close() {
	if c.connection != nil {
		_ = c.connection.Close()
	}
}
