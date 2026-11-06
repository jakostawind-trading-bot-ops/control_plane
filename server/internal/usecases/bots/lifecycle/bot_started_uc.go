package bot_usecases

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
)

type BotStartedUC struct {
	Logger *slog.Logger
}

func (BotStartedUC *BotStartedUC) Execute(ctx context.Context) error {
	Logger := BotStartedUC.Logger.With("operation", "bot_started_use_case")
	Tracer := otel.Tracer(
		"github.com/jakostawind-trading-bot-ops/control_plane/server/internal/usecases/bots/lifecycle/bot_started_uc.go",
	)
	ctx, span := Tracer.Start(ctx, "BotStartedUC.Execute")
	defer span.End()

	Logger.DebugContext(ctx, "BotStartedUC выполнился")
	return nil
}
