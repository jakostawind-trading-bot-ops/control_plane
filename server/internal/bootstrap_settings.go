package internal

import (
	"fmt"
	"os"
	"strings"
)

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

	SessionCookieSecure bool // Флаг Secure у cookie; отключается только для локального HTTP.
}

// LoadBootstrapSettings читает окружение перед запуском Fx. Если обязательная
// переменная пуста или содержит только пробельные символы, функция возвращает
// нулевые настройки и ошибку с именем переменной.
// Secure у cookie включён по умолчанию; SESSION_COOKIE_SECURE=false отключает его.
func LoadBootstrapSettings() (BootstrapSettings, error) {
	settings := BootstrapSettings{
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

		SessionCookieSecure: os.Getenv("SESSION_COOKIE_SECURE") != "false",
	}

	for _, field := range []struct{ name, value string }{
		{"NATS_URL", settings.NatsUrl},
		{"LOGGER_LEVEL", settings.LoggerLevel},
		{"VALKEY_HOST", settings.ValkeyHost},
		{"VALKEY_PORT", settings.ValkeyPort},
		{"CONTROL_USERNAME", settings.ControlUsername},
		{"CONTROL_PASSWORD", settings.ControlPassword},
	} {
		if strings.TrimSpace(field.value) == "" {
			return BootstrapSettings{}, fmt.Errorf("обязательная переменная %s не задана", field.name)
		}
	}
	return settings, nil
}
