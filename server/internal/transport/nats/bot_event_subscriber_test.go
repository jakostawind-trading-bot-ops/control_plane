package nats

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	events "github.com/jakostawind-trading-bot-ops/control_plane/internal/events/bots"
	"github.com/nats-io/nats.go"
)

const subscriberTestTimeout = 3 * time.Second // Ограничивает ожидания внутри тестов.

// subscriberDecoderFunc подставляет функцию декодирования в тестируемый подписчик.
type subscriberDecoderFunc func(context.Context, string, []byte) (events.BaseBotEventI, error)

// BotEventDecode передаёт ctx, kind и raw тестовой функции и возвращает её событие или ошибку.
func (f subscriberDecoderFunc) BotEventDecode(ctx context.Context, kind string, raw []byte) (events.BaseBotEventI, error) {
	return f(ctx, kind, raw)
}

// subscriberDispatcherFunc подставляет функцию обработки события.
type subscriberDispatcherFunc func(context.Context, events.BaseBotEventI) error

// BotEventDispatch передаёт ctx и event тестовой функции и возвращает её ошибку.
func (f subscriberDispatcherFunc) BotEventDispatch(ctx context.Context, event events.BaseBotEventI) error {
	return f(ctx, event)
}

// subscriberJetStreamStub заменяет только Subscribe; остальные методы не используются.
type subscriberJetStreamStub struct {
	nats.JetStreamContext                                                                           // Дополняет интерфейс методами, которые эти тесты не вызывают.
	subscribe             func(string, nats.MsgHandler, ...nats.SubOpt) (*nats.Subscription, error) // Поведение подписки в сценарии.
}

// Subscribe передаёт subject, handler и opts заглушке и возвращает подписку или ошибку.
func (s subscriberJetStreamStub) Subscribe(subject string, handler nats.MsgHandler, opts ...nats.SubOpt) (*nats.Subscription, error) {
	return s.subscribe(subject, handler, opts...)
}

// subscriberPipeDialer подключает настоящий NATS-клиент к тестовому соединению в памяти.
type subscriberPipeDialer struct {
	conn net.Conn // Клиентская сторона net.Pipe; повторное подключение запрещено опцией NoReconnect.
}

// Dial возвращает соединение в памяти; network и address не используются.
func (d subscriberPipeDialer) Dial(network, address string) (net.Conn, error) {
	return d.conn, nil
}

// runSubscriberNatsPeer обслуживает минимальный протокол для проверки Ack/Term.
// conn — серверная сторона net.Pipe; publications получает отправленные клиентом PUB.
// Возвращает ошибку чтения, записи или неподдерживаемой команды и закрывает conn.
// Это заглушка транспорта: хранение и повторную доставку JetStream она не моделирует.
//
// Схема:
//
//	NATS-клиент -> conn -> [PING: PONG / PUB: publications / служебная команда: пропуск]
func runSubscriberNatsPeer(conn net.Conn, publications chan<- *nats.Msg) error {
	defer conn.Close()
	if _, err := io.WriteString(conn, "INFO {\"server_id\":\"subscriber-test\",\"version\":\"2.10.0\",\"proto\":1,\"max_payload\":1048576}\r\n"); err != nil {
		return err
	}
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			return errors.New("пустая команда NATS")
		}
		switch fields[0] {
		case "CONNECT", "SUB", "UNSUB", "PONG":
			// Подписки нужны клиенту для привязки Msg; входящие события создаёт тест.
		case "PING":
			if _, err := io.WriteString(conn, "PONG\r\n"); err != nil {
				return err
			}
		case "PUB":
			if len(fields) != 3 && len(fields) != 4 {
				return fmt.Errorf("неверная команда PUB: %q", line)
			}
			size, err := strconv.Atoi(fields[len(fields)-1])
			if err != nil || size < 0 || size > 1048576 {
				return fmt.Errorf("неверный размер PUB: %q", line)
			}
			payload := make([]byte, size+2)
			if _, err := io.ReadFull(reader, payload); err != nil {
				return err
			}
			if string(payload[size:]) != "\r\n" {
				return errors.New("PUB не завершается CRLF")
			}
			select {
			case publications <- &nats.Msg{Subject: fields[1], Data: payload[:size]}:
			default:
				return errors.New("слишком много подтверждений сообщения")
			}
		default:
			return fmt.Errorf("неподдерживаемая команда NATS: %q", line)
		}
	}
}

// newSubscriberNatsConnection создаёт соединение и подписку для настоящих Msg.Ack/Term.
// t получает ошибки настройки и автоматически закрывает соединение и goroutine.
// Возвращает клиент, действующую подписку и очередь исходящих публикаций.
func newSubscriberNatsConnection(t *testing.T) (*nats.Conn, *nats.Subscription, <-chan *nats.Msg) {
	t.Helper()
	client, server := net.Pipe()
	publications := make(chan *nats.Msg, 4)
	done := make(chan error, 1)
	go func() { done <- runSubscriberNatsPeer(server, publications) }()
	var conn *nats.Conn
	t.Cleanup(func() {
		if conn != nil {
			conn.Close()
		}
		client.Close()
		server.Close()
		select {
		case err := <-done:
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, net.ErrClosed) {
				t.Errorf("заглушка NATS: %v", err)
			}
		case <-time.After(subscriberTestTimeout):
			t.Error("заглушка NATS не завершилась")
		}
	})
	var err error
	conn, err = nats.Connect(nats.DefaultURL, nats.SetCustomDialer(subscriberPipeDialer{conn: client}), nats.NoReconnect(), nats.Timeout(subscriberTestTimeout))
	if err != nil {
		t.Fatalf("подключение NATS в памяти: %v", err)
	}
	subscription, err := conn.SubscribeSync("subscriber.test")
	if err != nil {
		t.Fatalf("создание подписки: %v", err)
	}
	return conn, subscription, publications
}

// newSubscriberForTest возвращает подписчик с очередью и логгером без внешних ресурсов.
func newSubscriberForTest() *NatsBotEventSubscriber {
	return &NatsBotEventSubscriber{
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		SubjectPrefix: "bot.*.to.control.event.",
		StreamName:    "BOT_EVENTS",
		ConsumerName:  "control_plane",
		MessagesQueue: make(chan *nats.Msg, 4),
	}
}

// closeSubscriberForTest останавливает s; t получает ошибку при неуспешной остановке.
func closeSubscriberForTest(t *testing.T, s *NatsBotEventSubscriber) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), subscriberTestTimeout)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close() = %v", err)
	}
}

// waitSubscriberQueueClosed ждёт закрытия пустой queue с ограничением времени; t получает ошибку ожидания.
func waitSubscriberQueueClosed(t *testing.T, queue <-chan *nats.Msg) {
	t.Helper()
	select {
	case message, ok := <-queue:
		if ok {
			t.Fatalf("очередь содержит необработанное сообщение: %v", message)
		}
	case <-time.After(subscriberTestTimeout):
		t.Fatal("очередь не закрыта")
	}
}

// TestSubscriberGetMessageType проверяет маршруты и ошибки subject; t управляет сценариями.
func TestSubscriberGetMessageType(t *testing.T) {
	tests := []struct {
		name    string // Название сценария.
		prefix  string // Префикс подписки.
		subject string // Входящий subject.
		want    string // Ожидаемый тип события.
		wantErr error  // Ошибка для проверки через errors.Is.
	}{
		{"wildcard", "bot.*.to.control.event.", "bot.bot-1.to.control.event.v1.lifecycle.started", "v1.lifecycle.started", nil},
		{"without trailing dot", "bot.*.to.control.event", "bot.bot-1.to.control.event.v1.started", "v1.started", nil},
		{"literal prefix", "bot.bot-1.", "bot.bot-1.started", "started", nil},
		{"missing type", "bot.*.to.control.event.", "bot.bot-1.to.control.event", "", MESSAGE_TYPE_NOT_FOUND},
		{"empty type", "bot.*.to.control.event.", "bot.bot-1.to.control.event.", "", MESSAGE_TYPE_NOT_FOUND},
		{"empty subject", "bot.*.to.control.event.", "", "", MESSAGE_TYPE_NOT_FOUND},
		{"wrong prefix", "bot.*.to.control.event.", "other.bot-1.to.control.event.started", "", SUBJECT_PREFIX_MISMATCH},
		{"wrong bot", "bot.bot-1.", "bot.bot-2.started", "", SUBJECT_PREFIX_MISMATCH},
		{"empty bot", "bot.*.to.control.event.", "bot..to.control.event.started", "", SUBJECT_PREFIX_MISMATCH},
		{"star in subject", "bot.*.to.control.event.", "bot.*.to.control.event.started", "", SUBJECT_PREFIX_MISMATCH},
		{"greater in subject", "bot.*.to.control.event.", "bot.bot>1.to.control.event.started", "", SUBJECT_PREFIX_MISMATCH},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newSubscriberForTest()
			s.SubjectPrefix = tt.prefix
			got, err := s.GetMessageType(t.Context(), tt.subject)
			if got != tt.want || !errors.Is(err, tt.wantErr) {
				t.Fatalf("GetMessageType(%q) = (%q, %v), want (%q, %v)", tt.subject, got, err, tt.want, tt.wantErr)
			}
		})
	}
}

// TestSubscriberStartErrors проверяет условия запуска; t управляет сценариями.
func TestSubscriberStartErrors(t *testing.T) {
	tests := []struct {
		name    string // Название сценария.
		closing bool   // Начата ли остановка.
		workers bool   // Созданы ли воркеры.
		wantErr error  // Ожидаемая ошибка запуска.
	}{
		{"closing", true, true, IS_CLOSING},
		{"workers not started", false, false, WORKERS_NOT_STARTED},
		{"not connected", false, true, NATS_NOT_CONNECTED},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newSubscriberForTest()
			s.Closing, s.WorkersStarted = tt.closing, tt.workers
			if err := s.Start(t.Context()); !errors.Is(err, tt.wantErr) {
				t.Fatalf("Start() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// TestSubscriberStart проверяет подписку и повторный запуск; t управляет проверками.
// Схема:
//
//	Start -> callback -> очередь; повторный Start -> прежняя подписка
func TestSubscriberStart(t *testing.T) {
	_, wantSubscription, _ := newSubscriberNatsConnection(t)
	s := newSubscriberForTest()
	s.WorkersStarted = true
	var callback nats.MsgHandler
	subscribeCalls := 0
	s.Jetstream = subscriberJetStreamStub{
		subscribe: func(subject string, handler nats.MsgHandler, _ ...nats.SubOpt) (*nats.Subscription, error) {
			subscribeCalls++
			if subject != "bot.*.to.control.event.>" {
				t.Errorf("subject = %q", subject)
			}
			callback = handler
			return wantSubscription, nil
		},
	}
	for range 2 {
		if err := s.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if s.Subscription != wantSubscription || callback == nil || subscribeCalls != 1 {
		t.Fatal("Start не сохранил подписку или создал повторную")
	}

	// Буфер позволяет проверить callback синхронно, без воркеров и ожиданий.
	message := &nats.Msg{Subject: "bot.bot-1.to.control.event.v1.started"}
	callback(message)
	select {
	case got := <-s.MessagesQueue:
		if got != message {
			t.Fatal("callback передал другое сообщение")
		}
	default:
		t.Fatal("callback не передал сообщение в очередь")
	}

	s.MessageQueueMutex.Lock()
	s.MessageQueueClosed = true
	close(s.MessagesQueue)
	s.MessageQueueMutex.Unlock()
	callback(message) // Отправка в закрытый канал вызовет panic и провалит тест.
}

// TestSubscriberStartSubscribeError проверяет сохранение причины ошибки; t принимает результат.
func TestSubscriberStartSubscribeError(t *testing.T) {
	s := newSubscriberForTest()
	s.WorkersStarted = true
	previous := &nats.Subscription{}
	s.Subscription = previous
	cause := errors.New("subscribe failed")
	s.Jetstream = subscriberJetStreamStub{
		subscribe: func(string, nats.MsgHandler, ...nats.SubOpt) (*nats.Subscription, error) {
			return nil, cause
		},
	}
	err := s.Start(t.Context())
	if !errors.Is(err, CANNOT_SUBSCRIBE) || !errors.Is(err, cause) {
		t.Fatalf("Start() = %v: потеряна категория или исходная причина", err)
	}
	if s.Subscription != previous {
		t.Fatal("ошибка Subscribe заменила прежнюю подписку")
	}
}

// TestSubscriberHandleMessage проверяет результат обработки и настоящие публикации Ack/Term.
// t управляет сценариями; неподтверждённое сообщение не должно создавать публикаций.
//
// Схема:
//
//	subject -> декодер -> диспетчер -> +ACK
//	              +-- ошибка, кроме Canceled -> +TERM
func TestSubscriberHandleMessage(t *testing.T) {
	tests := []struct {
		name         string // Название сценария.
		subject      string // Пустое значение заменяется корректным subject.
		decodeErr    error  // Ответ декодера.
		dispatchErr  error  // Ответ диспетчера.
		unbound      bool   // Сообщение не связано с подпиской для проверки ошибки отправки.
		wantDecode   int    // Ожидаемое число декодирований.
		wantDispatch int    // Ожидаемое число передач события.
		wantAck      string // Ожидаемые байты подтверждения; пусто, если отправка запрещена или не удалась.
		wantLog      error  // Ожидаемая категория ошибки в логе; nil при успехе.
	}{
		{name: "success", wantDecode: 1, wantDispatch: 1, wantAck: "+ACK"},
		{name: "invalid subject", subject: "wrong", wantLog: MESSAGE_TYPE_NOT_FOUND},
		{name: "prefix mismatch", subject: "other.bot-1.to.control.event.v1.started", wantLog: SUBJECT_PREFIX_MISMATCH},
		{name: "decode error", decodeErr: errors.New("invalid payload"), wantDecode: 1, wantAck: "+TERM", wantLog: CANNOT_DECODE_MESSAGE},
		{name: "decode canceled", decodeErr: context.Canceled, wantDecode: 1, wantLog: context.Canceled},
		{name: "decode wrapped canceled", decodeErr: fmt.Errorf("decode: %w", context.Canceled), wantDecode: 1, wantLog: context.Canceled},
		{name: "dispatch error", dispatchErr: errors.New("handler failed"), wantDecode: 1, wantDispatch: 1, wantLog: CANNOT_DISPATCH_EVENT},
		{name: "dispatch canceled", dispatchErr: context.Canceled, wantDecode: 1, wantDispatch: 1, wantLog: context.Canceled},
		{name: "dispatch wrapped canceled", dispatchErr: fmt.Errorf("dispatch: %w", context.Canceled), wantDecode: 1, wantDispatch: 1, wantLog: context.Canceled},
		{name: "ack error", unbound: true, wantDecode: 1, wantDispatch: 1, wantLog: CANNOT_ACK_MESSAGE},
		{name: "term error", unbound: true, decodeErr: errors.New("invalid payload"), wantDecode: 1, wantLog: CANNOT_TERMINATE_MESSAGE},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn, subscription, publications := newSubscriberNatsConnection(t)
			s := newSubscriberForTest()
			var logs bytes.Buffer
			s.Logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			event := &events.BaseBotEvent{BotID: "bot-1"}
			payload := []byte("payload")
			decodeCalls, dispatchCalls := 0, 0
			s.NatsBotEventDecoder = subscriberDecoderFunc(func(_ context.Context, kind string, raw []byte) (events.BaseBotEventI, error) {
				decodeCalls++
				if kind != "v1.started" || !bytes.Equal(raw, payload) {
					t.Errorf("декодер получил (%q, %q)", kind, raw)
				}
				if tt.decodeErr != nil {
					return nil, tt.decodeErr
				}
				return event, nil
			})
			s.BotEventDispatcher = subscriberDispatcherFunc(func(_ context.Context, got events.BaseBotEventI) error {
				dispatchCalls++
				if got != event {
					t.Error("диспетчер получил другое событие")
				}
				return tt.dispatchErr
			})
			subject := tt.subject
			if subject == "" {
				subject = "bot.bot-1.to.control.event.v1.started"
			}
			message := &nats.Msg{Subject: subject, Data: payload, Reply: "subscriber.ack", Sub: subscription}
			if tt.unbound {
				message.Sub = nil
			}
			s.HandleMessage(t.Context(), message)
			if decodeCalls != tt.wantDecode || dispatchCalls != tt.wantDispatch {
				t.Fatalf("decode/dispatch = %d/%d, want %d/%d", decodeCalls, dispatchCalls, tt.wantDecode, tt.wantDispatch)
			}
			// PONG приходит после всех предыдущих PUB: отсутствие публикаций проверяется без sleep.
			if err := conn.FlushTimeout(subscriberTestTimeout); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if tt.wantAck != "" {
				select {
				case publication := <-publications:
					if publication.Subject != message.Reply || string(publication.Data) != tt.wantAck {
						t.Fatalf("подтверждение = %q %q, want %q %q", publication.Subject, publication.Data, message.Reply, tt.wantAck)
					}
				default:
					t.Fatalf("не отправлено подтверждение %q", tt.wantAck)
				}
			}
			select {
			case publication := <-publications:
				t.Fatalf("лишнее подтверждение: %q", publication.Data)
			default:
			}
			if tt.wantLog != nil && !strings.Contains(logs.String(), tt.wantLog.Error()) {
				t.Errorf("в логе нет %v: %s", tt.wantLog, logs.String())
			}
			if tt.unbound && !strings.Contains(logs.String(), nats.ErrMsgNotBound.Error()) {
				t.Error("в логе потеряна исходная ошибка Ack/Term")
			}
		})
	}
}

// TestSubscriberCreateWorkersErrors проверяет валидацию пула; t управляет сценариями.
func TestSubscriberCreateWorkersErrors(t *testing.T) {
	tests := []struct {
		name     string // Название сценария.
		count    int    // Запрошенное число воркеров.
		closing  bool   // Начата ли остановка.
		started  bool   // Уже создан ли пул.
		nilQueue bool   // Отсутствует ли очередь.
		wantErr  error  // Ожидаемая ошибка.
	}{
		{"zero", 0, false, false, false, INVALID_WORKER_COUNT},
		{"negative", -1, false, false, false, INVALID_WORKER_COUNT},
		{"closing", 1, true, false, false, IS_CLOSING},
		{"already started", 1, false, true, false, WORKERS_ALREADY_STARTED},
		{"no queue", 1, false, false, true, MESSAGE_QUEUE_NOT_CREATED},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newSubscriberForTest()
			s.Closing, s.WorkersStarted = tt.closing, tt.started
			if tt.nilQueue {
				s.MessagesQueue = nil
			}
			if err := s.CreateWorkers(t.Context(), tt.count); !errors.Is(err, tt.wantErr) {
				t.Fatalf("CreateWorkers() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// TestSubscriberWorkersDrainQueue проверяет обработку после ошибки; t принимает результаты.
// Схема:
//
//	очередь с ошибочным и корректными сообщениями -> воркер -> Close -> все результаты
func TestSubscriberWorkersDrainQueue(t *testing.T) {
	s := newSubscriberForTest()
	t.Cleanup(func() { closeSubscriberForTest(t, s) })
	processed := make(chan string, 2)
	s.NatsBotEventDecoder = subscriberDecoderFunc(func(_ context.Context, _ string, raw []byte) (events.BaseBotEventI, error) {
		if string(raw) == "bad" {
			return nil, errors.New("invalid message")
		}
		return &events.BaseBotEvent{BotID: string(raw)}, nil
	})
	s.BotEventDispatcher = subscriberDispatcherFunc(func(_ context.Context, event events.BaseBotEventI) error {
		select {
		case processed <- event.IsBaseBotEvent().BotID:
		default:
			t.Error("воркер обработал лишнее сообщение")
		}
		return nil
	})
	// Один воркер обязан продолжить чтение после ошибочного сообщения.
	for _, payload := range []string{"bad", "bot-1", "bot-2"} {
		s.MessagesQueue <- &nats.Msg{Subject: "bot.bot-1.to.control.event.v1.started", Data: []byte(payload)}
	}
	if err := s.CreateWorkers(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	closeSubscriberForTest(t, s)
	if len(processed) != 2 {
		t.Fatalf("обработано %d сообщений, want 2", len(processed))
	}
	if first, second := <-processed, <-processed; first != "bot-1" || second != "bot-2" {
		t.Fatalf("обработаны %q, %q", first, second)
	}
	waitSubscriberQueueClosed(t, s.MessagesQueue)
}

// TestSubscriberClose проверяет остановку и её повторение; t управляет сценариями.
func TestSubscriberClose(t *testing.T) {
	tests := []struct {
		name     string // Название сценария.
		nilQueue bool   // Отсутствует ли очередь.
		badSub   bool   // Возвращает ли Drain ошибку неактивной подписки.
	}{
		{"without subscription", false, false},
		{"without queue", true, false},
		{"drain error", false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newSubscriberForTest()
			if tt.nilQueue {
				s.MessagesQueue = nil
			}
			if tt.badSub {
				s.Subscription = &nats.Subscription{}
			}
			ctx, cancel := context.WithTimeout(t.Context(), subscriberTestTimeout)
			defer cancel()
			for range 2 {
				err := s.Close(ctx)
				if tt.badSub {
					if !errors.Is(err, CLOSE_SUBSCRIPTION) || !errors.Is(err, nats.ErrBadSubscription) {
						t.Fatalf("Close() = %v: потеряна категория или причина Drain", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
			}
			if !s.Closing || !s.MessageQueueClosed {
				t.Fatal("Close не отметил остановку и закрытие очереди")
			}
			if s.MessagesQueue != nil {
				waitSubscriberQueueClosed(t, s.MessagesQueue)
			}
		})
	}
}

// TestSubscriberCloseContext проверяет отмену ожидания и продолжение фоновой очистки.
// t управляет сценариями; обработчик освобождается даже при досрочном завершении теста.
//
// Схема:
//
//	обработчик заблокирован -> Close -> отмена ожидания -> освобождение -> завершение Close
func TestSubscriberCloseContext(t *testing.T) {
	for _, wantErr := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(wantErr.Error(), func(t *testing.T) {
			s := newSubscriberForTest()
			entered, release := make(chan struct{}), make(chan struct{})
			releaseHandler := sync.OnceFunc(func() { close(release) })
			t.Cleanup(func() {
				releaseHandler()
				closeSubscriberForTest(t, s)
			})
			s.NatsBotEventDecoder = subscriberDecoderFunc(func(context.Context, string, []byte) (events.BaseBotEventI, error) {
				return &events.BaseBotEvent{}, nil
			})
			s.BotEventDispatcher = subscriberDispatcherFunc(func(context.Context, events.BaseBotEventI) error {
				close(entered)
				<-release
				return nil
			})
			if err := s.CreateWorkers(t.Context(), 1); err != nil {
				t.Fatal(err)
			}
			s.MessagesQueue <- &nats.Msg{Subject: "bot.bot-1.to.control.event.v1.started"}
			select {
			case <-entered:
			case <-time.After(subscriberTestTimeout):
				t.Fatal("воркер не начал обработку")
			}

			var ctx context.Context
			var cancel context.CancelFunc
			if wantErr == context.Canceled {
				ctx, cancel = context.WithCancel(t.Context())
			} else {
				ctx, cancel = context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
			}
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- s.Close(ctx) }()
			// Закрытие очереди доказывает, что фоновая остановка уже началась.
			waitSubscriberQueueClosed(t, s.MessagesQueue)
			if wantErr == context.Canceled {
				select {
				case err := <-result:
					t.Fatalf("Close завершился до отмены при работающем обработчике: %v", err)
				default:
				}
				cancel()
			}
			select {
			case err := <-result:
				if !errors.Is(err, wantErr) {
					t.Fatalf("Close() = %v, want %v", err, wantErr)
				}
			case <-time.After(subscriberTestTimeout):
				t.Fatal("Close не прервал ожидание по контексту")
			}
			select {
			case <-s.CloseDone:
				t.Fatal("очистка завершилась до выхода обработчика")
			default:
			}
			if err := s.Start(t.Context()); !errors.Is(err, IS_CLOSING) {
				t.Fatalf("Start после Close = %v", err)
			}
			if err := s.CreateWorkers(t.Context(), 1); !errors.Is(err, IS_CLOSING) {
				t.Fatalf("CreateWorkers после Close = %v", err)
			}
			releaseHandler()
			closeSubscriberForTest(t, s)
		})
	}
}
