package shared

import (
	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
	"go.uber.org/fx"
)

func BootstrapSettingsProvider(BootstrapSettings *internal.BootstrapSettings) fx.Option{
	return fx.Supply(BootstrapSettings)
}