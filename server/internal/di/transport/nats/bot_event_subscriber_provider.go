package di_transport_nats

import (
	"context"
	"log/slog"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal/ports"
	bots_nats "github.com/jakostawind-trading-bot-ops/control_plane/internal/transport/nats"
	"github.com/nats-io/nats.go"
	"go.uber.org/fx"
)

const (
	NATS_BOT_EVENT_STREAM         = "BOT_TO_CONTROL_EVENTS"              // Существующий stream событий ботов, к которому Start привязывает подписку через nats.Bind.
	NATS_BOT_EVENT_CONSUMER       = "control_plane_bot_event_subscriber" // Существующий consumer в stream; провайдер не создаёт его.
	NATS_BOT_EVENT_SUBJECT_PREFIX = "bot.*.to.control.event."            // * занимает токен ID бота; Start добавляет > для суффикса типа события.

	BOT_EVENT_WORKERS_COUNT = 4 // Число goroutine, обрабатывающих сообщения из общей очереди.
)

// NatsBotEventSubscriberProvider регистрирует в lc запуск воркеров и подписчика.
// GetJetStream даёт снимок общего соединения. Decoder и Dispatcher обрабатывают
// сообщения в воркерах, Logger получает ошибки повторных попыток.
// Возвращает порт подписчика. Fx может завершить запуск до подключения NATS.
//
//	OnStart -> CreateWorkers -> Run -> ожидание замен и повторов в фоне
//	OnStop  -> Close -> Drain -> завершение воркеров -> отмена их контекста
func NatsBotEventSubscriberProvider(
	lc fx.Lifecycle,
	GetJetStream BotEventJetStreamFactory,
	Decoder ports.BotEventDecoderPort,
	Dispatcher ports.BotEventDispatcherPort,
	Logger *slog.Logger,
) ports.BotEventSubscriberPort {
	// Этап 1: очередь принадлежит подписчику; Close закроет её после остановки приёма.
	Logger = Logger.With(
		"component", "bot_event_subscriber",
		"stream", NATS_BOT_EVENT_STREAM,
		"consumer", NATS_BOT_EVENT_CONSUMER,
	)
	Subscriber := &bots_nats.NatsBotEventSubscriber{
		Logger:              Logger,
		Snapshot:            GetJetStream,
		StreamName:          NATS_BOT_EVENT_STREAM,
		ConsumerName:        NATS_BOT_EVENT_CONSUMER,
		SubjectPrefix:       NATS_BOT_EVENT_SUBJECT_PREFIX,
		NatsBotEventDecoder: Decoder,
		BotEventDispatcher:  Dispatcher,
		MessagesQueue:       make(chan *nats.Msg, bots_nats.MESSAGES_QUEUE_LIMIT),
	}
	var cancelWorkers context.CancelFunc
	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			ctx, cancel := context.WithCancel(context.Background())
			cancelWorkers = cancel
			if err := Subscriber.CreateWorkers(ctx, BOT_EVENT_WORKERS_COUNT); err != nil {
				cancel()
				return err
			}
			if err := Subscriber.Run(ctx); err != nil {
				cancel()
				return err
			}
			return nil
		},
		OnStop: func(ctx context.Context) error {
			defer cancelWorkers()
			return Subscriber.Close(ctx)
		},
	})
	return Subscriber
}
