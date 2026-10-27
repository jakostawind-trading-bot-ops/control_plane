package nats

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/events"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/ports"
)

// NatsBotEventDecoder выбирает декодер входящего сообщения по его типу.
// Маршруты передаются при сборке зависимостей; конкретные контракты и их
// преобразование во внутренние события находятся в функциях Decode маршрутов.
type NatsBotEventDecoder struct {
	Logger *slog.Logger // Общий JSON-логгер из DI; должен быть задан до обработки сообщений.
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
	Logger := NatsBotEventDecoder.Logger.With("operation", "decode_bot_event", "message_type", MessageType)
	// Этап 1: выбираем маршрут конкретного типа и версии сообщения.
	Logger.DebugContext(ctx, "Поиск маршрута декодирования", "payload_size", len(RawMessage))
	Route, ok := NatsBotEventDecoder.BotEventDecoderRoutes[MessageType]
	if !ok {
		Logger.DebugContext(ctx, "Маршрут декодирования не найден")
		return nil, fmt.Errorf("маршрут для типа %q не зарегистрирован", MessageType)
	}

	// Этап 2: маршрут разбирает, проверяет и преобразует свой контракт.
	Logger.DebugContext(ctx, "Декодирование и валидация контракта")
	BotEvent, err := Route.Decode(RawMessage)
	if err != nil {
		Logger.DebugContext(ctx, "Контракт не декодирован")
		return nil, fmt.Errorf(
			"обработка сообщения %q: %w",
			MessageType, err,
		)
	}

	Logger.DebugContext(ctx, "Событие декодировано", "event_type", fmt.Sprintf("%T", BotEvent))
	return BotEvent, nil
}

// botEventDecodeErrorLogValue готовит безопасное значение ошибки декодирования для лога.
// err — ненулевая ошибка маршрута; исходная ошибка, возвращаемая вызывающему коду,
// не меняется. Для ошибок JSON результат содержит только категорию и смещение:
// Error(), JSONPointer и JSONValue могут включать чувствительные данные payload.
// Остальные ошибки текущих маршрутов содержат тип сообщения или причину валидации.
func botEventDecodeErrorLogValue(err error) slog.Value {
	var SyntaxError *jsontext.SyntacticError
	var SemanticError *json.SemanticError
	switch {
	case errors.As(err, &SyntaxError):
		return slog.GroupValue(
			slog.String("kind", "json_syntax"),
			slog.Int64("byte_offset", SyntaxError.ByteOffset),
		)
	case errors.As(err, &SemanticError):
		return slog.GroupValue(
			slog.String("kind", "json_semantic"),
			slog.Int64("byte_offset", SemanticError.ByteOffset),
		)
	default:
		return slog.StringValue(err.Error())
	}
}
