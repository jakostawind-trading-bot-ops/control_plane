package main

import (
	di_bots "github.com/jakostawind-trading-bot-ops/control_plane/cmd/api/di/modules/bots"
	di_bots_nats "github.com/jakostawind-trading-bot-ops/control_plane/cmd/api/di/modules/bots/nats"
	"github.com/jakostawind-trading-bot-ops/control_plane/cmd/api/di/shared"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
	dispatchroutes "github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/events/dispatch_routes"
	eventroutes "github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/nats/decoder_routes"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/ports"
	"go.uber.org/fx"
)

// main собирает зависимости и передаёт запуск и остановку компонентов Fx.
func main() {
	bootstrap_settings := internal.LoadBootstrapSettings()

	app := fx.New(
		di.BootstrapSettingsProvider(&bootstrap_settings), // настройки запуска

		fx.Provide(di.NatsClientProvider), // NatsClient

		// Bots Module
		fx.Provide( // BotEventDecoder
			eventroutes.BotEventDecoderRoutesRegistry,
			di_bots.BotEventDecoderProvider,
		),

		fx.Provide( // BotEventDispatcher
			dispatchroutes.BotEventDispatchRoutesRegistry,
			di_bots.BotEventDispatcherProvider,
		),
		fx.Provide(
			di_bots_nats.NatsBotEventJetStreamProvider,
			di_bots_nats.NatsBotEventSubscriberProvider,
		),
		fx.Invoke(func(_ ports.BotEventSubscriberPort) {}),
	)

	app.Run()
}
