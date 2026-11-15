package di_repositories

import (
	"context"
	"log/slog"

	di "github.com/jakostawind-trading-bot-ops/control_plane/internal/di/storage"
	repositories "github.com/jakostawind-trading-bot-ops/control_plane/internal/repositories/sessions"
	"go.uber.org/fx"
)

func SessionsRepoProvider(
	lc fx.Lifecycle,
	Logger *slog.Logger,
	ValkeyClient *di.ValkeyClient,
) repositories.SessionsRepo {
	SessionsRepo := &repositories.SessionsValkeyRepo{
		Logger: Logger.With("component", "session_valkey_repo"),
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			SessionsRepo.Client = ValkeyClient.Client
			return nil
		},
	})

	return SessionsRepo
}
