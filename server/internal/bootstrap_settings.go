package internal

import "os"

// BootstrapSettings содержит настройки запуска из переменных окружения.
type BootstrapSettings struct {
	Stage           string // Окружение приложения из STAGE.
	LoggerLevel     string // Уровень логирования
	NatsUrl         string // Адрес NATS из NATS_URL.
	NatsUser        string // Имя пользователя NATS из NATS_USER.
	NatsPassword    string // Пароль NATS из NATS_PASSWORD; не предназначен для логов.
	ValkeyHost      string
	ValkeyPort      string
	ValkeyPassword  string
	ControlUsername string
	ControlPassword string
}

// LoadBootstrapSettings возвращает настройки из окружения без проверки значений.
// Отсутствующая переменная даёт пустую строку; значения секретов не логируются.
func LoadBootstrapSettings() BootstrapSettings {
	return BootstrapSettings{
		Stage:           os.Getenv("STAGE"),
		LoggerLevel:     os.Getenv("LOGGER_LEVEL"),
		NatsUrl:         os.Getenv("NATS_URL"),
		NatsUser:        os.Getenv("NATS_USER"),
		NatsPassword:    os.Getenv("NATS_PASSWORD"),
		ValkeyHost:      os.Getenv("VALKEY_HOST"),
		ValkeyPort:      os.Getenv("VALKEY_PORT"),
		ValkeyPassword:  os.Getenv("VALKEY_PASSWORD"),
		ControlUsername: os.Getenv("CONTROL_USERNAME"),
		ControlPassword: os.Getenv("CONTROL_PASSWORD"),
	}
}
