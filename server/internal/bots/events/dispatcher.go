package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
)

var (
	// HANDLER_NOT_FOUND означает, что для конкретного Go-типа события не зарегистрирован обработчик.
	HANDLER_NOT_FOUND = errors.New("BotEventDispatcher: обработчик события не зарегистрирован")
	// CANNOT_HANDLE_EVENT обозначает отказ обработчика в логах и span; исходная ошибка возвращается без изменения.
	CANNOT_HANDLE_EVENT = errors.New("BotEventDispatcher: не удалось обработать событие")
)

// BotEventDispatcher синхронно передаёт событие обработчику из реестра DI.
// Зависимости задаются до запуска воркеров и не меняются во время обработки.
type BotEventDispatcher struct {
	Logger   *slog.Logger                      // Общий JSON-логгер из DI; обязателен для обработки.
	Handlers map[reflect.Type]BotEventHandlerI // Обработчики по конкретному Go-типу события.
}

// BotEventDispatch выбирает обработчик и возвращает результат его выполнения.
//
// Параметры:
//   - ctx — контекст обработки, передаваемый логгеру и обработчику.
//   - BotEvent — внутреннее событие, конкретный тип которого определяет маршрут.
//
// Результат — ошибка отсутствующего маршрута либо исходная ошибка обработчика.
// Итог операции логирует вызывающий подписчик; здесь фиксируются этапы и ошибки.
//
// Этапы:
//  1. Найти обработчик по конкретному Go-типу.
//  2. Вызвать обработчик и вернуть его результат без изменения ошибки.
//
// Схема:
//
//	BotEvent -> [Поиск обработчика] +-- не найден -> error
//	                               +-- найден -> [HandleBotEvent] -> nil / error
func (BotEventDispatcher *BotEventDispatcher) BotEventDispatch(ctx context.Context, BotEvent BaseBotEventI) error {
	Started := time.Now()

	Logger := BotEventDispatcher.Logger.With("operation", "dispatch_bot_event", "event_type", fmt.Sprintf("%T", BotEvent))

	Tracer := otel.Tracer(
		"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/events",
	)
	ctx, span := Tracer.Start(ctx, "BotEventDispatcher.BotEventDispatch")
	defer span.End()

	// Этап 1: диспетчер работает с внутренним типом, а не версией транспортного контракта.
	Logger.DebugContext(ctx, "Поиск обработчика события")
	Handler, ok := BotEventDispatcher.Handlers[reflect.TypeOf(BotEvent)]

	if !ok {
		Logger.ErrorContext(
			ctx, "Обработчик события не зарегистрирован",
			"error", HANDLER_NOT_FOUND,
			"duration_ms", time.Since(Started).Milliseconds(),
		)
		span.RecordError(HANDLER_NOT_FOUND)
		span.SetStatus(codes.Error, HANDLER_NOT_FOUND.Error())
		return HANDLER_NOT_FOUND
	}

	// Этап 2: подписчик владеет итоговым логом и решением о подтверждении сообщения.
	Logger.DebugContext(ctx, "Вызов обработчика события")
	if err := Handler.HandleBotEvent(ctx, BotEvent); err != nil {
		Logger.ErrorContext(
			ctx, "Обработчик вернул ошибку",
			"error", CANNOT_HANDLE_EVENT,
			"cause", err,
			"duration_ms", time.Since(Started).Milliseconds(),
		)
		span.RecordError(CANNOT_HANDLE_EVENT)
		span.SetStatus(codes.Error, CANNOT_HANDLE_EVENT.Error())
		return CANNOT_HANDLE_EVENT
	}
	Logger.DebugContext(ctx, "Обработчик события завершён", "duration_ms", time.Since(Started).Milliseconds())
	return nil
}
