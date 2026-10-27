package di_observability

import (
	"log/slog"
	"os"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/observability/logging"
	"go.uber.org/fx"
)

func JSONLoggerProvider(
	lc fx.Lifecycle,
	BootstrapSettings *internal.BootstrapSettings,
) (*slog.Logger, error) {
	Logger := &logging.Logger{
		BootstrapSettings: *BootstrapSettings,
	}

	return Logger.CreateJSONLogger(os.Stdout)
}