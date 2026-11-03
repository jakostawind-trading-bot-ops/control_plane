package events

import (
	"context"
	"log/slog"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/events"
	usecases "github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/usecases/lifecycle"
	"go.opentelemetry.io/otel"
)

// BotStartedEvent представляет сообщение о запуске бота без привязки к версии контракта.
type BotStartedEvent struct {
	events.BaseBotEvent // Идентификатор бота, время и payload входящего события.
}

// BotStartedEventHandler пока фиксирует получение события; use case ещё не подключён.
type BotStartedEventHandler struct {
	Logger       *slog.Logger // Общий JSON-логгер из DI; payload события в логи не передаётся.
	BotStartedUC usecases.BotStartedUC
}

var _ events.BotEventHandlerI = (*BotStartedEventHandler)(nil)

// HandleBotEvent фиксирует вызов обработчика-заглушки и возвращает nil.
//
// Параметры:
//   - ctx — контекст обработки для структурированных логов.
//   - BotEvent — событие от диспетчера; для *BotStartedEvent логируется BotID.
//
// Метод не изменяет состояние бота и не подтверждает выполнение use case.
func (Handler *BotStartedEventHandler) HandleBotEvent(
	ctx context.Context,
	BotEvent events.BaseBotEventI,
) error {
	// Started := time.Now
	Logger := Handler.Logger.With("operation", "handle_bot_started_event")
	if Event, ok := BotEvent.(*BotStartedEvent); ok && Event != nil {
		Logger = Logger.With("bot_id", Event.BotID)
	}
	Tracer := otel.Tracer(
		"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/events",
	)
	ctx, span := Tracer.Start(ctx, "BotStartedEventHandler.HandleBotEvent")
	defer span.End()
	Logger.DebugContext(ctx, "Событие запуска бота получено обработчиком")
	Handler.BotStartedUC.Execute(ctx)
	return nil
}
