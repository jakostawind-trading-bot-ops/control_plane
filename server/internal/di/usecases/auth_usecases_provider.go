package di

import (
	"log/slog"

	auth_usecases "github.com/jakostawind-trading-bot-ops/control_plane/internal/usecases/auth"
)

func LoginUCProvider(Logger *slog.Logger) *auth_usecases.LoginUC {
	return &auth_usecases.LoginUC{
		Logger: Logger.With("component", "login_use_case"),
	}
}