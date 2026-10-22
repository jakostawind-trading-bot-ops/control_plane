package main

import (
	di_bots "github.com/jakostawind-trading-bot-ops/control_plane/cmd/api/di/modules/bots"
	"github.com/jakostawind-trading-bot-ops/control_plane/cmd/api/di/shared"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
	dispatchroutes "github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/events/dispatch_routes"
	eventroutes "github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/events/event_routes"
	"go.uber.org/fx"
)

func main() {
	bootstrap_settings := internal.LoadBootstrapSettings()

	app := fx.New(
		di.BootstrapSettingsProvider(&bootstrap_settings), // настройки запуска

		fx.Provide(di.NatsClientProvider), // NatsClient
		fx.Invoke(func(ctx *di.NatsClient) {}),

		// Bots Module
		fx.Provide( // BotEventDecoder
			eventroutes.BotEventDecoderRoutesRegistry,
			di_bots.BotEventDecoderProvider,
		),

		fx.Provide( // BotEventDispatcher
			dispatchroutes.BotEventDispatchRoutesRegistry,
			di_bots.BotEventDispatcherProvider,
		),
	)

	app.Run()
}
