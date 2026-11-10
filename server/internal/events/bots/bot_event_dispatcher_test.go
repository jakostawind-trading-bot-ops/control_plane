package bot_events

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"testing"
)

// botEventHandlerFunc адаптирует тестовую функцию к контракту обработчика события.
type botEventHandlerFunc func(context.Context, BaseBotEventI) error

// HandleBotEvent передаёт ctx и event тестовой функции и возвращает её ошибку.
func (handler botEventHandlerFunc) HandleBotEvent(ctx context.Context, event BaseBotEventI) error {
	return handler(ctx, event)
}

// dispatcherTestEvent позволяет отличить конкретный тип события от базового.
type dispatcherTestEvent struct {
	BaseBotEvent // Общие данные и реализация BaseBotEventI для тестового события.
}

// TestBotEventDispatcher_BotEventDispatch_Success проверяет выбор маршрута и передачу входных данных.
// t управляет тестом; обработчик должен получить исходное событие и значение из ctx ровно один раз.
func TestBotEventDispatcher_BotEventDispatch_Success(t *testing.T) {
	type contextKey int
	const requestKey contextKey = 0
	ctx := context.WithValue(t.Context(), requestKey, "request-1")
	wantEvent := &dispatcherTestEvent{BaseBotEvent: BaseBotEvent{BotID: "bot-1"}}
	calls := 0
	dispatcher := &BotEventDispatcher{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Handlers: map[reflect.Type]BotEventHandlerI{
			reflect.TypeOf(&BaseBotEvent{}): botEventHandlerFunc(func(context.Context, BaseBotEventI) error {
				t.Fatal("диспетчер вызвал обработчик базового типа вместо конкретного типа события")
				return nil
			}),
			reflect.TypeOf(wantEvent): botEventHandlerFunc(func(handlerCtx context.Context, event BaseBotEventI) error {
				calls++
				if event != wantEvent {
					t.Errorf("событие = %#v, ожидалось исходное событие %#v", event, wantEvent)
				}
				if got := handlerCtx.Value(requestKey); got != "request-1" {
					t.Errorf("значение контекста = %v, ожидалось request-1", got)
				}
				return nil
			}),
		},
	}

	if err := dispatcher.BotEventDispatch(ctx, wantEvent); err != nil {
		t.Fatalf("BotEventDispatch вернул неожиданную ошибку: %v", err)
	}
	if calls != 1 {
		t.Errorf("число вызовов обработчика = %d, ожидалось 1", calls)
	}
}

// TestBotEventDispatcher_BotEventDispatch_HandlerNotFound проверяет отказ при отсутствии маршрута.
// t управляет тестом; nil, пустой реестр и маршрут другого типа не должны приводить к вызову обработчика.
func TestBotEventDispatcher_BotEventDispatch_HandlerNotFound(t *testing.T) {
	unexpectedHandler := botEventHandlerFunc(func(context.Context, BaseBotEventI) error {
		t.Error("диспетчер вызвал обработчик незарегистрированного типа события")
		return nil
	})
	tests := []struct {
		name     string                            // Имя проверяемого случая отсутствия маршрута.
		event    BaseBotEventI                     // Входное событие, для которого маршрут отсутствует.
		handlers map[reflect.Type]BotEventHandlerI // Реестр, не содержащий обработчик входного типа.
	}{
		{
			name:  "nil_registry",
			event: &BaseBotEvent{BotID: "bot-1"},
		},
		{
			name:     "empty_registry",
			event:    &BaseBotEvent{BotID: "bot-1"},
			handlers: map[reflect.Type]BotEventHandlerI{},
		},
		{
			name:  "different_concrete_type",
			event: &dispatcherTestEvent{BaseBotEvent: BaseBotEvent{BotID: "bot-1"}},
			handlers: map[reflect.Type]BotEventHandlerI{
				reflect.TypeOf(&BaseBotEvent{}): unexpectedHandler,
			},
		},
		{
			name: "nil_event",
			handlers: map[reflect.Type]BotEventHandlerI{
				reflect.TypeOf(&BaseBotEvent{}): unexpectedHandler,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dispatcher := &BotEventDispatcher{
				Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
				Handlers: tt.handlers,
			}

			if err := dispatcher.BotEventDispatch(t.Context(), tt.event); err != HANDLER_NOT_FOUND {
				t.Errorf("ошибка = %v, ожидалась HANDLER_NOT_FOUND", err)
			}
		})
	}
}

// TestBotEventDispatcher_BotEventDispatch_HandlerError проверяет возврат CANNOT_HANDLE_EVENT при ошибке обработчика.
// t управляет тестом; диспетчер должен вызвать обработчик один раз и вернуть собственную ошибку.
func TestBotEventDispatcher_BotEventDispatch_HandlerError(t *testing.T) {
	event := &BaseBotEvent{BotID: "bot-1"}
	handlerErr := errors.New("ошибка обработчика")
	calls := 0
	dispatcher := &BotEventDispatcher{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Handlers: map[reflect.Type]BotEventHandlerI{
			reflect.TypeOf(event): botEventHandlerFunc(func(context.Context, BaseBotEventI) error {
				calls++
				return handlerErr
			}),
		},
	}

	if err := dispatcher.BotEventDispatch(t.Context(), event); err != CANNOT_HANDLE_EVENT {
		t.Errorf("ошибка = %v, ожидалась CANNOT_HANDLE_EVENT", err)
	}
	if calls != 1 {
		t.Errorf("число вызовов обработчика = %d, ожидалось 1", calls)
	}
}
