package nats

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	events "github.com/jakostawind-trading-bot-ops/control_plane/internal/events/bots"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/ports"
)

// TestNatsBotEventDecoder_BotEventDecode_Success проверяет передачу данных маршруту и возврат его события.
// t управляет тестом; маршрут должен получить исходные байты и значение из контекста.
func TestNatsBotEventDecoder_BotEventDecode_Success(t *testing.T) {
	type contextKey int
	const requestKey contextKey = 0
	ctx := context.WithValue(context.Background(), requestKey, "request-1")
	const messageType = "bot.started.v1"
	rawMessage := []byte(" { \"bot_id\": \"bot-1\" }\n")
	wantEvent := &events.BaseBotEvent{BotID: "bot-1"}

	decoder := &NatsBotEventDecoder{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		BotEventDecoderRoutes: map[string]ports.BotEventDecoderRoute{
			messageType: {
				MessageType: messageType,
				Decode: func(routeCtx context.Context, raw []byte) (events.BaseBotEventI, error) {
					if !bytes.Equal(raw, rawMessage) {
						t.Errorf("маршрут получил %q, ожидалось %q", raw, rawMessage)
					}
					if got := routeCtx.Value(requestKey); got != "request-1" {
						t.Errorf("значение контекста = %v, ожидалось request-1", got)
					}
					return wantEvent, nil
				},
			},
		},
	}

	gotEvent, err := decoder.BotEventDecode(ctx, messageType, rawMessage)
	if err != nil {
		t.Fatalf("BotEventDecode вернул неожиданную ошибку: %v", err)
	}
	if gotEvent != wantEvent {
		t.Errorf("событие = %#v, ожидалось исходное событие %#v", gotEvent, wantEvent)
	}
}

// TestNatsBotEventDecoder_BotEventDecode_UnknownRoute проверяет отказ для незарегистрированного типа.
// t управляет тестом; существующий маршрут не должен подменять отсутствующий.
func TestNatsBotEventDecoder_BotEventDecode_UnknownRoute(t *testing.T) {
	decoder := &NatsBotEventDecoder{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		BotEventDecoderRoutes: map[string]ports.BotEventDecoderRoute{
			"bot.started.v1": {
				MessageType: "bot.started.v1",
				Decode: func(context.Context, []byte) (events.BaseBotEventI, error) {
					t.Fatal("декодер вызвал маршрут другого типа сообщения")
					return nil, nil
				},
			},
		},
	}

	gotEvent, err := decoder.BotEventDecode(context.Background(), "bot.unknown.v1", []byte("{}"))
	if !errors.Is(err, DECODER_ROUTE_NOT_FOUND) {
		t.Errorf("ошибка = %v, ожидалась DECODER_ROUTE_NOT_FOUND", err)
	}
	if gotEvent != nil {
		t.Errorf("при отсутствии маршрута получено событие: %#v", gotEvent)
	}
}

// TestNatsBotEventDecoder_BotEventDecode_RouteError проверяет сохранение ошибки маршрута.
// t управляет тестом; ошибка должна поддерживать errors.Is/errors.As и содержать тип сообщения.
func TestNatsBotEventDecoder_BotEventDecode_RouteError(t *testing.T) {
	const messageType = "bot.started.v1"
	decodeErr := &json.SemanticError{
		ByteOffset: 12,
		Err:        errors.New("invalid bot_id"),
	}
	partialEvent := &events.BaseBotEvent{BotID: "bot-1"}
	decoder := &NatsBotEventDecoder{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		BotEventDecoderRoutes: map[string]ports.BotEventDecoderRoute{
			messageType: {
				MessageType: messageType,
				Decode: func(context.Context, []byte) (events.BaseBotEventI, error) {
					return partialEvent, decodeErr
				},
			},
		},
	}

	gotEvent, err := decoder.BotEventDecode(context.Background(), messageType, []byte("{}"))
	if err == nil {
		t.Fatal("ожидалась ошибка декодирования")
	}
	if gotEvent != nil {
		t.Errorf("при ошибке получено частичное событие: %#v", gotEvent)
	}
	if !errors.Is(err, decodeErr) {
		t.Errorf("исходная ошибка потеряна: %v", err)
	}
	var semanticErr *json.SemanticError
	if !errors.As(err, &semanticErr) || semanticErr != decodeErr {
		t.Errorf("исходная json.SemanticError недоступна через errors.As: %v", err)
	}
	if errors.Is(err, DECODER_ROUTE_NOT_FOUND) {
		t.Error("ошибка существующего маршрута подменена DECODER_ROUTE_NOT_FOUND")
	}
	if !strings.Contains(err.Error(), messageType) {
		t.Errorf("ошибка не содержит тип сообщения %q: %v", messageType, err)
	}
}
