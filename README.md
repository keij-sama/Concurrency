# In-Memory Key-Value Database with WAL and Replication

Распределенная in-memory key-value база данных с поддержкой WAL (Write-Ahead Log) и репликацией Master-Slave.

## Особенности

- In-memory хранение с разделением на партиции для повышения параллелизма
- Write-Ahead Log (WAL) для обеспечения долговечности данных
- Поддержка репликации Master-Slave для высокой доступности
- Сетевой TCP-интерфейс для доступа к базе данных
- CLI-клиент для удобного взаимодействия
- Гибкие настройки через конфигурационные файлы

## Установка

### Предварительные требования

- Go 1.18 или выше
- Git

### Клонирование репозитория

```bash
git clone https://github.com/keij-sama/Concurrency
cd Concurrency
```

### Сборка

```bash
go build -o bin/server.exe ./database/cmd/server
go build -o bin/client.exe ./database/cmd/client
go build -o bin/cli.exe ./database/cmd/cli
```

## Конфигурация

Cоздайте конфигурационные файлы:

### config.yaml - Основная конфигурация

```yaml
engine:
  type: "in_memory"
network:
  address: "127.0.0.1:3223"
  max_connections: 100
  max_message_size: "4KB"
  idle_timeout: 5m
logging:
  level: "info"
  output: "stdout"
wal:
  enabled: true
  flushing_batch_size: 100
  flushing_batch_timeout: "10ms"
  max_segment_size: "10MB"
  data_directory: "/data/wal"
replication:
  enabled: true
  replica_type: "master"  # или "slave"
  master_address: "127.0.0.1:3232"
  sync_interval: "1s"
```

### master-config.yaml - Конфигурация для мастера

```yaml
engine:
  type: "in_memory"
network:
  address: "127.0.0.1:3223"
  max_connections: 100
  max_message_size: "4KB"
  idle_timeout: 5m
logging:
  level: "info"
  output: "stdout"
wal:
  enabled: true
  flushing_batch_size: 100
  flushing_batch_timeout: "10ms"
  max_segment_size: "10MB"
  data_directory: "./data/master/wal"
replication:
  enabled: true
  replica_type: "master"
  master_address: "127.0.0.1:3232"
  sync_interval: "1s"
```

### slave-config.yaml - Конфигурация для слейва

```yaml
engine:
  type: "in_memory"
network:
  address: "127.0.0.1:3224"
  max_connections: 100
  max_message_size: "4KB"
  idle_timeout: 5m
logging:
  level: "info"
  output: "stdout"
wal:
  enabled: true
  flushing_batch_size: 100
  flushing_batch_timeout: "10ms"
  max_segment_size: "10MB"
  data_directory: "./data/slave/wal"
replication:
  enabled: true
  replica_type: "slave"
  master_address: "127.0.0.1:3223"
  sync_interval: "1s"
```

## Запуск

### Локальный CLI режим

```powershell
.\bin\cli.exe --config config.yaml
```

### Сервер

```powershell
# Запуск в обычном режиме
.\bin\server.exe --config config.yaml

# Запуск как мастер
.\bin\server.exe --config master-config.yaml

# Запуск как слейв
.\bin\server.exe --config slave-config.yaml
```

### Клиент

```powershell
# Подключение к серверу по умолчанию
.\bin\client.exe

# Подключение к конкретному серверу
.\bin\client.exe --address 127.0.0.1:3223
```

## Использование

База данных поддерживает следующие команды:

- `SET key value` - установка значения для ключа
- `GET key` - получение значения по ключу
- `DEL key` - удаление ключа и его значения

### Примеры

```
> SET user1 John
OK
> GET user1
John
> DEL user1
OK
> GET user1
ERROR: key not found
```

## Структура проекта

```
database/
├── cmd/
│   ├── cli/         # CLI-интерфейс
│   ├── client/      # TCP-клиент
│   └── server/      # TCP-сервер
├── internal/
│   ├── config/      # Конфигурация
│   ├── database/
│   │   ├── compute/ # Обработка запросов
│   │   │   └── parser/ # Парсер команд
│   │   └── storage/ # Хранение данных
│   │       ├── engine/     # Движки хранения
│   │       ├── replication/ # Репликация
│   │       └── wal/        # Write-Ahead Log
│   └── network/     # Сетевое взаимодействие
└── pkg/
    └── logger/      # Логирование
```

## Настройка репликации

Для настройки репликации выполните следующие шаги:

1. Создайте директории для WAL:
   ```powershell
   mkdir -p ./data/master/wal
   mkdir -p ./data/slave/wal
   ```

2. Запустите мастер:
   ```powershell
   .\bin\server.exe --config master-config.yaml
   ```

3. Запустите слейв:
   ```powershell
   .\bin\server.exe --config slave-config.yaml
   ```

4. Подключитесь к мастеру и выполните операции:
   ```powershell
   .\bin\client.exe --address 127.0.0.1:3223
   ```

5. Проверьте репликацию, подключившись к слейву:
   ```powershell
   .\bin\client.exe --address 127.0.0.1:3223
   ```

## Ограничения

- В режиме слейва поддерживаются только операции чтения (GET)
- WAL должен быть включен для использования репликации
- Репликация синхронная и может влиять на производительность

## Примечания

- При перезапуске данные будут восстановлены из WAL, если он включен
- Для высокой производительности при работе с большим объемом данных рекомендуется увеличить размер буфера и batch-размер WAL
