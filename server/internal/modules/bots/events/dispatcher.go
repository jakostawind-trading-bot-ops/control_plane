package events

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
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
// Итог операции логирует вызывающий подписчик; здесь фиксируются только этапы.
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
	Logger := BotEventDispatcher.Logger.With("operation", "dispatch_bot_event", "event_type", fmt.Sprintf("%T", BotEvent))
	// Этап 1: диспетчер работает с внутренним типом, а не версией транспортного контракта.
	Logger.DebugContext(ctx, "Поиск обработчика события")
	Handler, ok := BotEventDispatcher.Handlers[reflect.TypeOf(BotEvent)]

	if !ok {
		Logger.DebugContext(ctx, "Обработчик события не зарегистрирован")
		return fmt.Errorf("обработчик события %T не зарегистрирован", BotEvent)
	}

	// Этап 2: подписчик владеет итоговым логом и решением о подтверждении сообщения.
	Logger.DebugContext(ctx, "Вызов обработчика события")
	if err := Handler.HandleBotEvent(ctx, BotEvent); err != nil {
		Logger.DebugContext(ctx, "Обработчик вернул ошибку")
		return err
	}
	Logger.DebugContext(ctx, "Обработчик события завершён")
	return nil
}
