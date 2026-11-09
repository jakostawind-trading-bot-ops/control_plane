package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/di"
	di_events "github.com/jakostawind-trading-bot-ops/control_plane/internal/di/events/bot"
	di_observability "github.com/jakostawind-trading-bot-ops/control_plane/internal/di/observability"
	di_storage "github.com/jakostawind-trading-bot-ops/control_plane/internal/di/storage"
	di_http "github.com/jakostawind-trading-bot-ops/control_plane/internal/di/transport/http"
	di_nats "github.com/jakostawind-trading-bot-ops/control_plane/internal/di/transport/nats"
	di_usecases "github.com/jakostawind-trading-bot-ops/control_plane/internal/di/usecases"
	dispatchroutes "github.com/jakostawind-trading-bot-ops/control_plane/internal/events/bots/dispatch_routes"
	tracing "github.com/jakostawind-trading-bot-ops/control_plane/internal/observability/tracing"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/ports"
	http_routes "github.com/jakostawind-trading-bot-ops/control_plane/internal/transport/http/routes"
	http_auth_routes_v1 "github.com/jakostawind-trading-bot-ops/control_plane/internal/transport/http/routes/v1/auth"
	eventroutes "github.com/jakostawind-trading-bot-ops/control_plane/internal/transport/nats/bot_events_decoder_routes"
	"go.uber.org/fx"
)

// main собирает зависимости и передаёт запуск и остановку компонентов Fx.
func main() {
	bootstrap_settings := internal.LoadBootstrapSettings()

	app := fx.New(
		di.BootstrapSettingsProvider(&bootstrap_settings), // Настройки запуска

		fx.Provide(di_observability.LoggerProvider), // OpenTelemetry logs
		fx.Provide(di_observability.TracerProvider), // OpenTelemetry tracing

		fx.Provide(di_nats.NatsClientProvider),      // NatsClient
		fx.Provide(di_storage.ValkeyClientProvider), // ValkeyClient

		fx.Provide(di_http.GinProvider),
		fx.Provide(
			func(GinEngine *gin.Engine) *http.Server {
				return di_http.CreateHTTPServer(":8000", GinEngine)
			},
		),

		// USECASES
		fx.Provide(
			// Bots
			di_usecases.BotStartedUCProvider,
			// Auth
			di_usecases.LoginUCProvider,
		),

		fx.Provide( // BotEventDecoder
			eventroutes.BotEventDecoderRoutesRegistry,
			di_nats.BotEventDecoderProvider,
		),

		fx.Provide( // BotEventDispatcher
			dispatchroutes.BotEventDispatchRoutesRegistry,
			di_events.BotEventDispatcherProvider,
		),
		fx.Provide(
			di_nats.NatsBotEventJetStreamProvider,
			di_nats.NatsBotEventSubscriberProvider,
		),

		fx.Provide(
			http_auth_routes_v1.HTTPAuthHandlersRegistry,
			http_routes.HTTPhandlersRegistry,
		),

		// Регистрируем lifecycle tracing до lifecycle подписчика.
		// Fx запускает хуки по порядку, а останавливает в обратном порядке.
		fx.Invoke(func(_ *tracing.Tracing) {}),
		fx.Invoke(func(_ *di_storage.ValkeyClient) {}),
		fx.Invoke(func(_ ports.BotEventSubscriberPort) {}),

		fx.Invoke(http_routes.RegisterHTTPRoutes),

		// HTTP останавливается до подписчика и observability.
		fx.Invoke(di_http.HTTPServerProvider),
	)

	app.Run()
}
