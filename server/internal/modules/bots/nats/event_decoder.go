package nats

import (
	"context"
	"fmt"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/events"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/ports"
)

// NatsBotEventDecoder выбирает декодер входящего сообщения по его типу.
// Маршруты передаются при сборке зависимостей; конкретные контракты и их
// преобразование во внутренние события находятся в функциях Decode маршрутов.
type NatsBotEventDecoder struct {
	// BotEventDecoderRoutes содержит маршруты с непустой функцией Decode по MessageType.
	// Карту не следует изменять одновременно с декодированием сообщений.
	BotEventDecoderRoutes map[string]ports.BotEventDecoderRoute
}

var _ ports.BotEventDecoderPort = (*NatsBotEventDecoder)(nil)

// BotEventDecode выбирает маршрут и передаёт ему исходные байты сообщения.
//
// Параметры:
//   - ctx — контекст из интерфейса порта; эта реализация не проверяет отмену.
//   - MessageType — точный ключ маршрута, включающий версию контракта.
//   - RawMessage — байты JSON, передаваемые выбранной функции Decode без изменений.
//
// Результаты:
//   - BaseBotEventI — внутреннее событие, возвращённое маршрутом.
//   - error — отсутствие маршрута либо ошибка Decode с указанием MessageType;
//     исходная ошибка сохраняется для errors.Is и errors.As.
//
// Этапы:
//  1. Найти маршрут по MessageType.
//  2. Вызвать Decode и добавить тип сообщения к ошибке маршрута.
//
// Схема:
//
//	MessageType -> [Поиск маршрута] --не найден--> error
//	                      |
//	                    найден
//	                      v
//	RawMessage -> [Route.Decode] -> событие / error
func (NatsBotEventDecoder *NatsBotEventDecoder) BotEventDecode(
	ctx context.Context,
	MessageType string,
	RawMessage []byte,
) (events.BaseBotEventI, error) {
	// Этап 1: выбираем маршрут конкретного типа и версии сообщения.
	Route, ok := NatsBotEventDecoder.BotEventDecoderRoutes[MessageType]
	if !ok {
		return nil, fmt.Errorf("маршрут для типа %q не зарегистрирован", MessageType)
	}

	// Этап 2: маршрут разбирает, проверяет и преобразует свой контракт.
	BotEvent, err := Route.Decode(RawMessage)
	if err != nil {
		return nil, fmt.Errorf(
			"обработка сообщения %q: %w",
			MessageType, err,
		)
	}

	return BotEvent, nil
}
