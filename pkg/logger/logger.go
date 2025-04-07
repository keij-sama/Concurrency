package logger

import (
	"go.uber.org/zap"
)

type Logger interface {
	Info(msg string, fields ...zap.Field)
	Error(msg string, fields ...zap.Field)
}

type ZapLogger struct {
	log *zap.Logger
}

func NewLogger() Logger {
	// Простая настройка для разработки
	logger, _ := zap.NewDevelopment()
	return &ZapLogger{
		log: logger,
	}
}

func NewLoggerWithZap(zapLogger *zap.Logger) Logger {
	return &ZapLogger{
		log: zapLogger,
	}
}

func (l *ZapLogger) Info(msg string, fields ...zap.Field) {
	l.log.Info(msg, fields...)
}

func (l *ZapLogger) Error(msg string, fields ...zap.Field) {
	l.log.Error(msg, fields...)
}
