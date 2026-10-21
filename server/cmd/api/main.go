package main

import (
	di_shared "github.com/jakostawind-trading-bot-ops/control_plane/cmd/api/di/shared"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
	"go.uber.org/fx"
)

func main() {
	bootstrap_settings := internal.LoadBootstrapSettings()

	app := fx.New(
		di_shared.BootstrapSettingsProvider(&bootstrap_settings), // настройки запуска

		fx.Provide(di_shared.NatsClientProvider), // NatsClient
		fx.Invoke(func(ctx *di_shared.NatsClient) {}),
	)

	app.Run()
}
