package di_usecases

import (
	"log/slog"

	sessions_repositories "github.com/jakostawind-trading-bot-ops/control_plane/internal/repositories/sessions"
	users_repositories "github.com/jakostawind-trading-bot-ops/control_plane/internal/repositories/users"
	auth_usecases "github.com/jakostawind-trading-bot-ops/control_plane/internal/usecases/auth"
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
	Logger *slog.Logger,
	UsersRepo users_repositories.UsersRepo,
	SessionsRepo sessions_repositories.SessionsRepo,
) *auth_usecases.LoginUC {

	return &auth_usecases.LoginUC{
		Logger:       Logger.With("component", "login_use_case"),
		UsersRepo:     UsersRepo,
		SessionsRepo: SessionsRepo,
	}
}
