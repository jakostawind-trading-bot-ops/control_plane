package main

import (
	"net/http"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
	dispatchroutes "github.com/jakostawind-trading-bot-ops/control_plane/internal/bots/events/dispatch_routes"
	eventroutes "github.com/jakostawind-trading-bot-ops/control_plane/internal/bots/nats/decoder_routes"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/bots/ports"
	di_bots "github.com/jakostawind-trading-bot-ops/control_plane/internal/di/modules/bots"
	di_bots_nats "github.com/jakostawind-trading-bot-ops/control_plane/internal/di/modules/bots/nats"
	di "github.com/jakostawind-trading-bot-ops/control_plane/internal/di/shared"
	di_observability "github.com/jakostawind-trading-bot-ops/control_plane/internal/di/shared/observability"
	tracing "github.com/jakostawind-trading-bot-ops/control_plane/internal/observability/tracing"
	"go.uber.org/fx"
)

// main собирает зависимости и передаёт запуск и остановку компонентов Fx.
func main() {
	bootstrap_settings := internal.LoadBootstrapSettings()

	app := fx.New(
		di.BootstrapSettingsProvider(&bootstrap_settings), // Настройки запуска

		fx.Provide(di_observability.LoggerProvider), // OpenTelemetry logs
		fx.Provide(di_observability.TracerProvider), // OpenTelemetry tracing

		fx.Provide(di.NatsClientProvider), // NatsClient

		fx.Provide(func() *http.Server {
			return di.CreateHTTPServer(":8080", http.NotFoundHandler())
		}),

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
		// Регистрируем lifecycle tracing до lifecycle подписчика.
		// Fx запускает хуки по порядку, а останавливает в обратном порядке.
		fx.Invoke(func(_ *tracing.Tracing) {}),
		fx.Invoke(func(_ ports.BotEventSubscriberPort) {}),
		// HTTP останавливается до подписчика и observability.
		fx.Invoke(di.HTTPServerProvider),
	)

	app.Run()
}
