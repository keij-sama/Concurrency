package parser

import (
	"errors"
	"strings"
)

// Константы для типов команд
const (
	CommandSet = "SET"
	CommandGet = "GET"
	CommandDel = "DEL"
)

// Cодержит типы команды (SET, GET, DEL) и список аргументов команды
type Command struct {
	Type      string
	Arguments []string
}

// Определяет поведение для любого парсера. Любой парсер должен реализовать метод Parse, который принимает строку и возвращает указатель на Command или ошибку
type Parser interface {
	Parse(input string) (*Command, error)
}

// Ошибки
var (
	ErrEmptyCommand        = errors.New("empty command")
	ErrInvalidCommand      = errors.New("invalid command")
	ErrInvalidArgumentsNum = errors.New("invalid number of arguments")
)

// Конкретная реализация парсера
type SimpleParser struct{}

// Создание нового парсера
func NewParser() Parser {
	return &SimpleParser{}
}

// Метод Parse для SimpleParser (реализует интерфейс Parser). Сначала удаляет начальные и конечные пробелы из входной строки, если после этого строка пуста, возвращает ошибку
func (p *SimpleParser) Parse(input string) (*Command, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, ErrEmptyCommand
	}

	// Разделяет входную строку на части
	parts := strings.Fields(input)

	// Извлекает тип команды (первое слово) и аргументы (все последующие слова)
	commandType := parts[0]
	args := parts[1:]

	// Проверяет тип команды
	if commandType != CommandSet && commandType != CommandGet && commandType != CommandDel {
		return nil, ErrInvalidCommand
	}

	// Проверяет количество аргументов
	if commandType == CommandSet && len(args) != 2 {
		return nil, ErrInvalidArgumentsNum
	}
	if (commandType == CommandGet || commandType == CommandDel) && len(args) != 1 {
		return nil, ErrInvalidArgumentsNum
	}

	// Если все проверки пройдены, создает и возвращает структуру Command с типом команды и аргументами
	return &Command{
		Type:      commandType,
		Arguments: args,
	}, nil
}
