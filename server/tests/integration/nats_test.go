//go:build integration

package integration_test

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	di_nats "github.com/jakostawind-trading-bot-ops/control_plane/internal/di/transport/nats"
	events "github.com/jakostawind-trading-bot-ops/control_plane/internal/events/bots"
	lifecycle "github.com/jakostawind-trading-bot-ops/control_plane/internal/events/bots/events/lifecycle"
	"github.com/nats-io/nats.go"
)

// TestNATSBotEventsPipeline проверяет все компоненты NATS на событиях разных ботов.
// t владеет изолированным брокером; проверяются поля события, выполнение use case,
// подтверждения на брокере и остановка настоящего Fx lifecycle.
//
// Схема:
//
//	JSON -> JetStream -> subscriber/workers -> decoder/contract
//	     -> dispatcher -> handler -> use case -> Ack -> Fx.Stop
func TestNATSBotEventsPipeline(t *testing.T) {
	h := newNATSHarness(t, nil)
	const count = 12
	want := make(map[string]string, count)
	var last uint64
	// Несколько ботов и больше сообщений, чем воркеров: нельзя полагаться на порядок доставки.
	for i := range count {
		botID, marker := fmt.Sprintf("bot-%d", i), fmt.Sprintf("payload-%d", i)
		want[botID] = marker
		subject, raw := startedMessage(botID, marker)
		last = h.publish(t, subject, raw)
	}
	for range count {
		event, ok := h.receiveEvent(t).(*lifecycle.BotStartedEvent)
		if !ok {
			t.Fatal("декодер или dispatcher передал неверный тип события")
		}
		marker, exists := want[event.BotID]
		if !exists {
			t.Fatalf("неожиданный или повторный bot_id: %q", event.BotID)
		}
		if event.Timestamp != "2026-11-10T12:00:00Z" || len(event.Payload) != 1 || event.Payload["marker"] != marker {
			t.Fatalf("поля события изменились при передаче: %+v", event)
		}
		delete(want, event.BotID)
	}
	h.waitConsumer(t, "все сообщения подтверждены Ack", func(info *nats.ConsumerInfo) bool {
		return info.AckFloor.Stream == last && info.NumAckPending == 0 && info.NumPending == 0
	})
	h.stop(t)
	if got := strings.Count(h.logs.String(), `"msg":"BotStartedUC выполнился"`); got != count {
		t.Errorf("use case выполнен %d раз, ожидалось %d", got, count)
	}
	select {
	case extra := <-h.handled:
		t.Fatalf("повторная обработка подтверждённого сообщения: %+v", extra)
	default:
	}
}

// TestNATSBotEventsRejectInvalidMessages проверяет Term на настоящем брокере.
// t управляет сценариями; повреждённый JSON, нарушение контракта и неизвестный тип
// не должны доходить до handler или мешать обработке следующего корректного события.
//
// Схема:
//
//	неверное сообщение -> decoder error -> Term -> advisory
//	корректное сообщение -> handler/use case -> Ack
func TestNATSBotEventsRejectInvalidMessages(t *testing.T) {
	h := newNATSHarness(t, nil)
	advisories, err := h.publisher.SubscribeSync("$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED." +
		di_nats.NATS_BOT_EVENT_STREAM + "." + di_nats.NATS_BOT_EVENT_CONSUMER)
	if err != nil {
		t.Fatal(err)
	}
	defer advisories.Unsubscribe()
	if err := h.publisher.FlushTimeout(natsTestTimeout); err != nil {
		t.Fatal(err)
	}
	subject, valid := startedMessage("bot-healthy", "healthy")
	cases := []struct {
		name    string // Проверяемая причина отказа декодера.
		subject string // Внешний subject сообщения.
		raw     string // JSON или намеренно повреждённые байты.
	}{
		{"malformed JSON", subject, `{"bot_id":`},
		{"missing bot_id", subject, `{"timestamp":"2026-11-10T12:00:00Z"}`},
		{"missing timestamp", subject, `{"bot_id":"bot-healthy"}`},
		{"unknown type", "bot.bot-healthy.to.control.event.v99.unknown", valid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sequence := h.publish(t, tc.subject, tc.raw)
			message, err := advisories.NextMsg(natsTestTimeout)
			if err != nil {
				t.Fatalf("нет подтверждения Term от брокера: %v", err)
			}
			var advisory struct {
				StreamSequence uint64 `json:"stream_seq"` // Номер сообщения, для которого брокер принял Term.
				Deliveries     uint64 `json:"deliveries"` // Число доставок до прекращения повторов.
			}
			if err := json.Unmarshal(message.Data, &advisory); err != nil {
				t.Fatal(err)
			}
			if advisory.StreamSequence != sequence || advisory.Deliveries != 1 {
				t.Fatalf("Term advisory = %+v, ожидался stream_seq=%d, deliveries=1", advisory, sequence)
			}
		})
	}
	last := h.publish(t, subject, valid)
	if event := h.receiveEvent(t).IsBaseBotEvent(); event.BotID != "bot-healthy" || event.Payload["marker"] != "healthy" {
		t.Fatalf("handler получил некорректное событие: %+v", event)
	}
	h.waitConsumer(t, "Term и Ack приняты брокером", func(info *nats.ConsumerInfo) bool {
		return info.AckFloor.Stream == last && info.NumAckPending == 0 && info.NumPending == 0
	})
	h.stop(t)
	if got := strings.Count(h.logs.String(), `"msg":"BotStartedUC выполнился"`); got != 1 {
		t.Errorf("use case выполнен %d раз вместо одного корректного события", got)
	}
	select {
	case event := <-h.handled:
		t.Fatalf("отклонённое сообщение дошло до handler: %+v", event)
	default:
	}
}

// TestNATSBotEventsRedeliverAfterHandlerError проверяет отсутствие Ack при ошибке dispatcher.
// t владеет брокером; только первая попытка handler завершается искусственной ошибкой,
// следующая проходит через настоящий handler/use case и подтверждается на брокере.
//
// Схема:
//
//	handler error -> без Ack -> AckWait -> повтор -> настоящий handler -> Ack
func TestNATSBotEventsRedeliverAfterHandlerError(t *testing.T) {
	var attempts atomic.Int32
	h := newNATSHarness(t, func(context.Context, events.BaseBotEventI) error {
		if attempts.Add(1) == 1 {
			return errors.New("временная ошибка обработчика в интеграционном тесте")
		}
		return nil
	})
	subject, raw := startedMessage("bot-retry", "retry")
	sequence := h.publish(t, subject, raw)
	if event := h.receiveEvent(t).IsBaseBotEvent(); event.BotID != "bot-retry" || event.Payload["marker"] != "retry" {
		t.Fatalf("повторная доставка изменила событие: %+v", event)
	}
	info := h.waitConsumer(t, "повторная доставка подтверждена", func(info *nats.ConsumerInfo) bool {
		return info.AckFloor.Stream == sequence && info.NumAckPending == 0
	})
	h.stop(t)
	if got := attempts.Load(); got != 2 || info.Delivered.Consumer != 2 {
		t.Errorf("попыток handler=%d, доставок брокера=%d, ожидалось 2/2", got, info.Delivered.Consumer)
	}
}

// TestNATSBotEventsShutdownWaitsForHandler проверяет остановку при незавершённой обработке.
// t управляет блокировкой handler; Fx.Stop должен дождаться его выхода, завершить
// воркеры и только затем закрыть клиент. Cleanup освобождает handler даже при ошибке теста.
//
// Схема:
//
//	handler ждёт -> Fx.Stop -> очередь закрыта, worker ещё работает
//	            -> release -> handler/use case -> worker завершён -> клиент закрыт
func TestNATSBotEventsShutdownWaitsForHandler(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	releaseHandler := sync.OnceFunc(func() { close(release) })
	var enteredOnce sync.Once
	h := newNATSHarness(t, func(ctx context.Context, _ events.BaseBotEventI) error {
		enteredOnce.Do(func() { close(entered) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	// Регистрируется после fixture: при сбое handler освобождается раньше её Fx.Stop.
	t.Cleanup(releaseHandler)
	subject, raw := startedMessage("bot-stopping", "shutdown")
	h.publish(t, subject, raw)
	select {
	case <-entered:
	case <-time.After(natsTestTimeout):
		t.Fatal("handler не начал обработку")
	}
	ctx, cancel := context.WithTimeout(context.Background(), natsTestTimeout)
	defer cancel()
	h.startStop()
	// Закрытие очереди — наблюдаемая граница после Drain, без произвольного sleep.
	select {
	case _, open := <-h.subscriber.MessagesQueue:
		if open {
			t.Fatal("в очереди неожиданно осталось сообщение")
		}
	case <-ctx.Done():
		t.Fatal("Fx.Stop не завершил Drain подписки")
	}
	select {
	case <-h.stopDone:
		t.Fatalf("Fx.Stop завершился до выхода handler: %v", h.stopErr)
	case <-time.After(100 * time.Millisecond):
		// Даём ошибочной реализации без ожидания workers время вернуть Stop;
		// мгновенный default мог бы скрыть её из-за планирования goroutine.
	}
	if !h.connection.IsConnected() {
		t.Error("соединение закрыто до завершения handler")
	}
	releaseHandler()
	h.stop(t)
	if event := h.receiveEvent(t).IsBaseBotEvent(); event.BotID != "bot-stopping" {
		t.Fatalf("завершён другой handler: %+v", event)
	}
	if !h.connection.IsClosed() {
		t.Error("NatsClient не закрыл соединение после завершения handler")
	}
}
