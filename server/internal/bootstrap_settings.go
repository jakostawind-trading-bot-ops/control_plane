package internal

import "os"

// BootstrapSettings содержит настройки запуска из переменных окружения.
type BootstrapSettings struct {
	Stage        string // Окружение приложения из STAGE.
	LoggerLevel  string // Уровень логирования
	NatsUrl      string // Адрес NATS из NATS_URL.
	NatsUser     string // Имя пользователя NATS из NATS_USER.
	NatsPassword string // Пароль NATS из NATS_PASSWORD; не предназначен для логов.
}

// LoadBootstrapSettings возвращает настройки из окружения без проверки значений.
// Отсутствующая переменная даёт пустую строку; значения секретов не логируются.
func LoadBootstrapSettings() BootstrapSettings {
	return BootstrapSettings{
		Stage:        os.Getenv("STAGE"),
		LoggerLevel:  os.Getenv("LOGGER_LEVEL"),
		NatsUrl:      os.Getenv("NATS_URL"),
		NatsUser:     os.Getenv("NATS_USER"),
		NatsPassword: os.Getenv("NATS_PASSWORD"),
	}
}
