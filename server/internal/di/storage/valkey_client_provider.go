package di

import (
	"context"
	"fmt"
	"net"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
	"github.com/valkey-io/valkey-go"
	"go.uber.org/fx"
)

// ValkeyClient хранит настройки и клиент, временем жизни которого управляет Fx.
// Использовать Client можно после успешного выполнения OnStart.
type ValkeyClient struct {
	BootstrapSettings internal.BootstrapSettings // Копия настроек подключения.
	Client            valkey.Client              // nil до успешного запуска; закрывается в OnStop.
}

// ValkeyClientProvider создаёт владельца клиента и регистрирует хуки Fx.
// lc — lifecycle приложения; BootstrapSettings — ненулевые настройки окружения.
// Возвращает экземпляр без подключения; ошибки подключения и PING возвращает OnStart.
// Пустой пароль допустим, если сервер не требует аутентификации.
//
// Этапы:
//  1. Скопировать настройки.
//  2. Зарегистрировать подключение, проверку доступности и закрытие.
//
// Схема:
//
//	OnStart -> NewClient -> PING -> сохранить Client
//	                         +-- ошибка -> Close -> вернуть ошибку
//	OnStop -> Close
func ValkeyClientProvider(
	lc fx.Lifecycle,
	BootstrapSettings *internal.BootstrapSettings,
) *ValkeyClient {
	// Этап 1: сохраняем настройки для подключения при запуске приложения.
	valkeyClient := &ValkeyClient{
		BootstrapSettings: *BootstrapSettings,
	}

	// Этап 2: Fx запускает клиент до зависимых компонентов и закрывает после них.
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			settings := valkeyClient.BootstrapSettings
			if settings.ValkeyHost == "" || settings.ValkeyPort == "" {
				return fmt.Errorf("Valkey: VALKEY_HOST и VALKEY_PORT должны быть заданы")
			}

			client, err := valkey.NewClient(valkey.ClientOption{
				InitAddress: []string{
					net.JoinHostPort(settings.ValkeyHost, settings.ValkeyPort),
				},
				Password: settings.ValkeyPassword,
			})
			if err != nil {
				return fmt.Errorf("Valkey: создание клиента: %w", err)
			}

			// При ошибке запуска Fx не вызовет OnStop этого хука,
			// поэтому освобождаем созданный клиент здесь.
			if err := client.Do(ctx, client.B().Ping().Build()).Error(); err != nil {
				client.Close()
				return fmt.Errorf("Valkey: проверка подключения: %w", err)
			}

			valkeyClient.Client = client
			return nil
		},
		OnStop: func(_ context.Context) error {
			if valkeyClient.Client != nil {
				valkeyClient.Client.Close()
			}
			return nil
		},
	})

	return valkeyClient
}
