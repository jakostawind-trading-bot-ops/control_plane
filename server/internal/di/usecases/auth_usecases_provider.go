package di

import (
	"context"
	"log/slog"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
	storage_di "github.com/jakostawind-trading-bot-ops/control_plane/internal/di/storage"
	sessions_repositories "github.com/jakostawind-trading-bot-ops/control_plane/internal/repositories/sessions"
	users_repositories "github.com/jakostawind-trading-bot-ops/control_plane/internal/repositories/users"
	auth_usecases "github.com/jakostawind-trading-bot-ops/control_plane/internal/usecases/auth"
	"go.uber.org/fx"
)

// LoginUCProvider собирает сценарий входа и подключает хранилище сессий.
// lc регистрирует инициализацию; Logger используется сценарием входа
// и репозиторием сессий;
// BootstrapSettings содержит учётные данные; ValkeyClient владеет подключением.
// Все зависимости должны быть ненулевыми.
// Возвращает LoginUC, готовый к обработке запросов после успешного OnStart.
//
// Схема:
//
//	ValkeyClient.OnStart -> SessionsRepo.Client -> запуск HTTP
func LoginUCProvider(
	lc fx.Lifecycle,
	Logger *slog.Logger,
	BootstrapSettings *internal.BootstrapSettings,
	ValkeyClient *storage_di.ValkeyClient,
) *auth_usecases.LoginUC {
	SessionsRepo := &sessions_repositories.SessionsValkeyRepo{
		Logger: Logger.With("component", "sessions_valkey_repo"),
	}

	// Хук зависимости ValkeyClient выполняется раньше этого хука.
	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			SessionsRepo.Client = ValkeyClient.Client
			return nil
		},
	})

	return &auth_usecases.LoginUC{
		Logger: Logger.With("component", "login_use_case"),
		UserRepo: &users_repositories.UsersBootstrapSettingsRepo{ // todo переделать, чтобы репозитории выдавались отдельно в di/repositories
			BootstrapSettings: BootstrapSettings,
		},
		SessionsRepo: SessionsRepo,
	}
}
