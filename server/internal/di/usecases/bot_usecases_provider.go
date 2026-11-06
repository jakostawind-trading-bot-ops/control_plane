package di

import (
	"log/slog"

	usecases "github.com/jakostawind-trading-bot-ops/control_plane/internal/usecases/bots/lifecycle"
)

func BotStartedUCProvider(Logger *slog.Logger) *usecases.BotStartedUC {
	return &usecases.BotStartedUC{
		Logger: Logger.With("component", "bot_started_use_case"),
	}
}
