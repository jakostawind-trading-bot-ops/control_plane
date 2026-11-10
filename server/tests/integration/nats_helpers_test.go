//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
	di_events "github.com/jakostawind-trading-bot-ops/control_plane/internal/di/events/bot"
	di_nats "github.com/jakostawind-trading-bot-ops/control_plane/internal/di/transport/nats"
	di_usecases "github.com/jakostawind-trading-bot-ops/control_plane/internal/di/usecases"
	events "github.com/jakostawind-trading-bot-ops/control_plane/internal/events/bots"
	dispatchroutes "github.com/jakostawind-trading-bot-ops/control_plane/internal/events/bots/dispatch_routes"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/ports"
	transport "github.com/jakostawind-trading-bot-ops/control_plane/internal/transport/nats"
	decoderroutes "github.com/jakostawind-trading-bot-ops/control_plane/internal/transport/nats/bot_events_decoder_routes"
	"github.com/nats-io/nats.go"
	"go.uber.org/fx"
)

const (
	natsImage       = "nats:2.14.3-alpine"             // Фиксированная версия брокера для воспроизводимости тестов.
	natsTestTimeout = 15 * time.Second                 // Предел ожидания отдельного сетевого или lifecycle-этапа.
	natsAckWait     = 2 * time.Second                  // Интервал повторной доставки неподтверждённого сообщения.
	natsEventType   = "v1.lifecycle.bot_started_event" // Тип события из внешнего контракта.
)

// natsHarness владеет приложением и отдельным соединением издателя с тестовым JetStream.
// Cleanup останавливает Fx раньше издателя и контейнера; повторные запросы Stop объединяются.
//
// Схема:
//
//	publisher -> [NATS/JetStream] -> [Fx: client -> subscriber -> decoder]
//	                                           -> dispatcher -> handler -> use case
//	Cleanup -> [Fx.Stop] -> [publisher.Close] -> [docker rm]
type natsHarness struct {
	app        *fx.App                           // Приложение с настоящими NATS-провайдерами и lifecycle.
	client     *di_nats.NatsClient               // Владелец соединения приложения, предоставленный DI.
	connection *nats.Conn                        // Соединение приложения для проверки закрытия после Fx.Stop.
	subscriber *transport.NatsBotEventSubscriber // Реальный подписчик для проверки завершения очереди.
	publisher  *nats.Conn                        // Независимое соединение, имитирующее отправителя событий.
	jetstream  nats.JetStreamContext             // API брокера для публикаций и проверки подтверждений.
	handled    chan events.BaseBotEventI         // События после успешного выполнения настоящего обработчика.
	logs       natsLogBuffer                     // Потокобезопасный вывод slog, доступный и при ошибке остановки.
	stopOnce   sync.Once                         // Разрешает ровно один запуск Fx.Stop, включая Cleanup после ошибки.
	stopDone   chan struct{}                     // Закрывается после возврата Fx.Stop; защищает чтение stopErr.
	stopErr    error                             // Результат остановки; читается только после stopDone.
}

// natsLogBuffer хранит диагностику slog и разрешает чтение при незавершённой очистке.
type natsLogBuffer struct {
	mu     sync.Mutex   // Сериализует запись логов и получение снимка.
	buffer bytes.Buffer // Накопленные строки JSON-логов приложения.
}

// Write сохраняет data от slog и возвращает число записанных байтов и nil.
func (b *natsLogBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

// String возвращает снимок логов без гонки с работающими воркерами.
func (b *natsLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

// eventHandlerFunc адаптирует тестовую обёртку к интерфейсу обработчика.
// Обёртка наблюдает данные и при необходимости вводит ошибку перед реальным handler.
type eventHandlerFunc func(context.Context, events.BaseBotEventI) error

// HandleBotEvent передаёт ctx обработки и event функции-обёртке, возвращая её ошибку.
func (handler eventHandlerFunc) HandleBotEvent(ctx context.Context, event events.BaseBotEventI) error {
	return handler(ctx, event)
}

// dockerCommand выполняет Docker CLI с ограничением времени.
// t получает диагностику ошибки; args задают команду без shell-интерпретации.
// Возвращает stdout без крайних пробелов; при ошибке завершает текущий тест.
func dockerCommand(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %v: %v\n%s", args, err, output)
	}
	return strings.TrimSpace(string(output))
}

// startNATSBroker запускает изолированный JetStream и возвращает настройки подключения.
// t владеет контейнером через Cleanup; рабочие NATS_URL и внешние stream не используются.
// Ошибка Docker, запуска или готовности брокера завершает тест, а не пропускает его.
//
// Этапы:
//  1. Создать контейнер с временным хранилищем и случайным loopback-портом.
//  2. Зарегистрировать очистку, запустить брокер и дождаться подключения с авторизацией.
//
// Схема:
//
//	docker create -> [Cleanup] -> docker start -> [готовность NATS] -> settings
func startNATSBroker(t *testing.T) *internal.BootstrapSettings {
	t.Helper()
	// Этап 1: каждый тест получает собственный брокер, включая stream с production-именами.
	container := dockerCommand(t, "create", "--pull=never", "--publish", "127.0.0.1::4222",
		natsImage, "--jetstream", "--store_dir", "/tmp/jetstream",
		"--user", "integration", "--pass", "integration-test-password")
	// Этап 2: очистка регистрируется до запуска, чтобы убрать контейнер и при сбое старта.
	t.Cleanup(func() {
		if t.Failed() {
			ctx, cancel := context.WithTimeout(context.Background(), natsTestTimeout)
			defer cancel()
			output, _ := exec.CommandContext(ctx, "docker", "logs", container).CombinedOutput()
			t.Logf("NATS server:\n%s", output)
		}
		ctx, cancel := context.WithTimeout(context.Background(), natsTestTimeout)
		defer cancel()
		if output, err := exec.CommandContext(ctx, "docker", "rm", "--force", "--volumes", container).CombinedOutput(); err != nil {
			t.Errorf("удаление тестового NATS: %v\n%s", err, output)
		}
	})
	dockerCommand(t, "start", container)
	address := dockerCommand(t, "port", container, "4222/tcp")
	settings := &internal.BootstrapSettings{
		NatsUrl: "nats://" + address, NatsUser: "integration", NatsPassword: "integration-test-password",
	}
	ctx, cancel := context.WithTimeout(t.Context(), natsTestTimeout)
	defer cancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		conn, err := nats.Connect(settings.NatsUrl, nats.UserInfo(settings.NatsUser, settings.NatsPassword),
			nats.NoReconnect(), nats.Timeout(250*time.Millisecond))
		if err == nil {
			conn.Close()
			return settings
		}
		select {
		case <-ctx.Done():
			t.Fatalf("NATS не готов: %v; последняя ошибка: %v", ctx.Err(), err)
		case <-ticker.C:
		}
	}
}

// newNATSHarness собирает реальную NATS-цепочку приложения через провайдеры из main.
// t владеет ресурсами; before необязателен и вызывается перед настоящим обработчиком
// для управляемой ошибки или блокировки. Возвращает запущенное приложение с подпиской.
//
// Этапы:
//  1. Подготовить брокер, независимого издателя и существующий push consumer.
//  2. Собрать Fx, обернув только handler для наблюдения результатов.
//  3. Запустить lifecycle и дождаться фактической привязки consumer.
//
// Схема:
//
//	[Брокер, stream, consumer] -> [Fx.Start] -> [WaitConnection, PushBound]
//	[Dispatcher] -> [before?] -> [настоящий handler/use case] -> handled
func newNATSHarness(t *testing.T, before eventHandlerFunc) *natsHarness {
	t.Helper()
	// Этап 1: Bind в production не создаёт инфраструктуру, её подготавливает fixture.
	settings := startNATSBroker(t)
	h := &natsHarness{handled: make(chan events.BaseBotEventI, 128), stopDone: make(chan struct{})}
	var err error
	h.publisher, err = nats.Connect(settings.NatsUrl, nats.UserInfo(settings.NatsUser, settings.NatsPassword),
		nats.NoReconnect(), nats.Timeout(natsTestTimeout))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.publisher.Close)
	h.jetstream, err = h.publisher.JetStream(nats.MaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.jetstream.AddStream(&nats.StreamConfig{
		Name: di_nats.NATS_BOT_EVENT_STREAM, Subjects: []string{"bot.*.to.control.event.>"}, Storage: nats.MemoryStorage,
	}); err != nil {
		t.Fatalf("AddStream: %v", err)
	}
	if _, err := h.jetstream.AddConsumer(di_nats.NATS_BOT_EVENT_STREAM, &nats.ConsumerConfig{
		Durable: di_nats.NATS_BOT_EVENT_CONSUMER, DeliverSubject: "integration.bot.events",
		FilterSubject: "bot.*.to.control.event.>", AckPolicy: nats.AckExplicitPolicy,
		AckWait: natsAckWait, MaxDeliver: 5, MaxAckPending: 128,
	}); err != nil {
		t.Fatalf("AddConsumer: %v", err)
	}

	// Этап 2: реальные registry, decoder, dispatcher, handler и use case остаются в графе.
	var subscriber ports.BotEventSubscriberPort
	h.app = fx.New(
		fx.NopLogger,
		fx.Supply(settings, slog.New(slog.NewJSONHandler(&h.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))),
		fx.Provide(
			di_nats.NatsClientProvider,
			di_nats.NatsBotEventJetStreamProvider,
			decoderroutes.BotEventDecoderRoutesRegistry,
			di_nats.BotEventDecoderProvider,
			di_usecases.BotStartedUCProvider,
			dispatchroutes.BotEventDispatchRoutesRegistry,
			di_events.BotEventDispatcherProvider,
			di_nats.NatsBotEventSubscriberProvider,
		),
		fx.Decorate(func(routes []ports.BotEventDispatchRoute) []ports.BotEventDispatchRoute {
			observed := append([]ports.BotEventDispatchRoute(nil), routes...)
			for i, route := range observed {
				next := route.Handler
				observed[i].Handler = eventHandlerFunc(func(ctx context.Context, event events.BaseBotEventI) error {
					if before != nil {
						if err := before(ctx, event); err != nil {
							return err
						}
					}
					if err := next.HandleBotEvent(ctx, event); err != nil {
						return err
					}
					select {
					case h.handled <- event:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
			}
			return observed
		}),
		fx.Populate(&h.client, &subscriber),
	)
	if err := h.app.Err(); err != nil {
		t.Fatalf("Fx graph: %v", err)
	}
	h.subscriber = subscriber.(*transport.NatsBotEventSubscriber)
	// Этап 3: OnStart асинхронный; готовность определяется через API самого брокера.
	ctx, cancel := context.WithTimeout(t.Context(), natsTestTimeout)
	defer cancel()
	if err := h.app.Start(ctx); err != nil {
		t.Fatalf("Fx.Start: %v", err)
	}
	t.Cleanup(func() {
		h.stop(t)
		if t.Failed() {
			t.Logf("Логи приложения:\n%s", h.logs.String())
		}
	})
	h.connection, err = h.client.WaitConnection(ctx)
	if err != nil {
		t.Fatalf("WaitConnection: %v", err)
	}
	h.waitConsumer(t, "подписка готова", func(info *nats.ConsumerInfo) bool { return info.PushBound })
	return h
}

// waitConsumer опрашивает состояние consumer до выполнения ready.
// t получает диагностику, description описывает ожидаемое состояние, ready проверяет
// снимок брокера. Возвращает подходящий снимок; при таймауте завершает тест с последним состоянием.
func (h *natsHarness) waitConsumer(t *testing.T, description string, ready func(*nats.ConsumerInfo) bool) *nats.ConsumerInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), natsTestTimeout)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var last *nats.ConsumerInfo
	var err error
	for {
		last, err = h.jetstream.ConsumerInfo(di_nats.NATS_BOT_EVENT_STREAM, di_nats.NATS_BOT_EVENT_CONSUMER, nats.Context(ctx))
		if err == nil && ready(last) {
			return last
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s: %v; consumer=%+v; ошибка=%v", description, ctx.Err(), last, err)
		case <-ticker.C:
		}
	}
}

// publish отправляет raw по subject через настоящий JetStream и возвращает stream sequence.
// t получает ошибку публикации; подтверждение PubAck доказывает сохранение сообщения брокером.
func (h *natsHarness) publish(t *testing.T, subject, raw string) uint64 {
	t.Helper()
	ack, err := h.jetstream.Publish(subject, []byte(raw))
	if err != nil {
		t.Fatalf("Publish(%s): %v", subject, err)
	}
	return ack.Sequence
}

// receiveEvent ждёт успешный вызов настоящего handler; t ограничивает ожидание тестом.
// Возвращает декодированное событие, не предполагая порядка работы воркеров.
func (h *natsHarness) receiveEvent(t *testing.T) events.BaseBotEventI {
	t.Helper()
	select {
	case event := <-h.handled:
		return event
	case <-time.After(natsTestTimeout):
		t.Fatal("handler не обработал событие")
		return nil
	}
}

// startStop запускает Fx.Stop один раз с отдельным контекстом и закрывает stopDone.
// Контекст не зависит от t.Context, поэтому остановка работает и из Cleanup.
//
// Схема:
//
//	первый вызов -> goroutine [Fx.Stop -> stopErr -> close(stopDone)]
//	повторный вызов -> без нового Fx.Stop
func (h *natsHarness) startStop() {
	h.stopOnce.Do(func() {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), natsTestTimeout)
			defer cancel()
			h.stopErr = h.app.Stop(ctx)
			close(h.stopDone)
		}()
	})
}

// stop ждёт единственную остановку Fx и проверяет освобождение ресурсов.
// t принимает ошибки; повторный вызов использует прежний результат Fx.Stop.
func (h *natsHarness) stop(t *testing.T) {
	t.Helper()
	h.startStop()
	<-h.stopDone
	if h.stopErr != nil {
		t.Errorf("Fx.Stop: %v", h.stopErr)
		return
	}
	if h.connection != nil && !h.connection.IsClosed() {
		t.Error("Fx.Stop не закрыл соединение NatsClient")
	}
	select {
	case _, open := <-h.subscriber.MessagesQueue:
		if open {
			t.Error("после Fx.Stop в очереди осталось сообщение")
		}
	default:
		t.Error("Fx.Stop не закрыл очередь подписчика")
	}
}

// startedMessage возвращает subject и JSON согласованного V1-контракта.
// botID определяет адресата и поле bot_id; marker позволяет обнаружить смешивание payload.
func startedMessage(botID, marker string) (string, string) {
	return fmt.Sprintf("bot.%s.to.control.event.%s", botID, natsEventType),
		fmt.Sprintf(`{"bot_id":%q,"timestamp":"2026-11-10T12:00:00Z","payload":{"marker":%q}}`, botID, marker)
}
