package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/keij-sama/CourseConcurrency/database/internal/database/compute"
	"github.com/keij-sama/CourseConcurrency/database/internal/database/compute/parser"
	"github.com/keij-sama/CourseConcurrency/database/internal/database/storage"
	"github.com/keij-sama/CourseConcurrency/database/internal/database/storage/engine"
)

func main() {
	// Инициализация компонентов
	parser := parser.NewParser()
	engine := engine.NewInMemoryEngine()
	storage := storage.NewStorage(engine)
	compute := compute.NewCompute(parser, storage)

	fmt.Println("In-memory Key-Value Database")
	fmt.Println("Available commands: SET, GET, DEL")
	fmt.Println("To exit, type exit or quit")
	fmt.Println()

	// Цикл обработки команд
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		input := scanner.Text()
		input = strings.TrimSpace(input)

		if input == "" {
			continue
		}

		// Проверка команды выхода
		if strings.ToLower(input) == "exit" || strings.ToLower(input) == "quit" {
			fmt.Println("Finishing work")
			break
		}

		// Обработка команды
		result, err := compute.Process(input)
		if err != nil {
			fmt.Printf("ERROR: %s\n", err)
		} else {
			fmt.Println(result)
		}
	}
}
