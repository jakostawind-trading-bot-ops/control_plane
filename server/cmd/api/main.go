package main

import (
	"github.com/jakostawind-trading-bot-ops/control_plane/cmd/api/di/shared"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
	"go.uber.org/fx"
)

func main() {
	bootstrap_settings := internal.LoadBootstrapSettings()

	app := fx.New(
		shared.BootstrapSettingsProvider(&bootstrap_settings), // настройки запуска

		fx.Provide(shared.NatsClientProvider),
		fx.Invoke(func(ctx *shared.NatsClient) {}),
	)

	app.Run()
}
