package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/keij-sama/Concurrency/database/internal/network"
)

func main() {
	// Парсим флаги командной строки
	address := flag.String("address", "127.0.0.1:3223", "Address of the database server")
	timeout := flag.Duration("timeout", 5*time.Minute, "Idle timeout for connection")
	flag.Parse()

	// Создаем клиента
	client, err := network.NewTCPClient(*address,
		network.WithClientIdleTimeout(*timeout))
	if err != nil {
		fmt.Printf("Error connection to server: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	fmt.Println("Connected to database server. Enter commands (SET, GET, DEL) or 'exit' to quit.")

	// Читаем команды от пользователя
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

		// Проверяем команду выхода
		if strings.ToLower(input) == "exit" || strings.ToLower(input) == "quit" {
			fmt.Println("Disconnecting from server")
			break
		}

		// Отправляем запрос на сервер
		response, err := client.Send([]byte(input))
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			break
		}

		fmt.Println(string(response))
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading input: %v\n", err)
	}
}
