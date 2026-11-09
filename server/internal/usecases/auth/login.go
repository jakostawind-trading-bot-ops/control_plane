package auth_usecases

import (
	"context"
	"log/slog"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
	"go.opentelemetry.io/otel"
)

type LoginUC struct {
	Logger *slog.Logger
	BootstrapSettings *internal.BootstrapSettings
}

func (LoginUC LoginUC) Execute(
	ctx context.Context,
	Username string,
	Password string,
) error {
	Logger := LoginUC.Logger.With("operation", "login_use_case")
	Tracer := otel.Tracer(
		"github.com/jakostawind-trading-bot-ops/control_plane/server/internal/usecases/auth/login.go",
	)
	ctx, span := Tracer.Start(ctx, "LoginUC.Execute")
	defer span.End()

	Logger.DebugContext(ctx, "LoginUC выполнился")
	return nil
}
