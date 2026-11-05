package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal/bots/ports"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
)

const (
	// MESSAGES_QUEUE_LIMIT задаёт ёмкость очереди сообщений, создаваемой DI-провайдером.
	MESSAGES_QUEUE_LIMIT = 100
)

// Errors
var (
	// NATS_NOT_CONNECTED означает, что Start вызван с nil в поле Jetstream.
	NATS_NOT_CONNECTED = errors.New("NatsBotEventSubscriber: нет подключения к NATS")
	// NATS_NOT_SUBSCRIBED объявлена для отсутствующей подписки; методы этого файла её не возвращают.
	NATS_NOT_SUBSCRIBED = errors.New("NatsBotEventSubscriber: нет подписки на subject")
	// CANNOT_SUBSCRIBE объединяется с исходной ошибкой Jetstream.Subscribe.
	CANNOT_SUBSCRIBE = errors.New("NatsBotEventSubscriber: не смог подписаться")
	// IS_CLOSING запрещает Start и CreateWorkers после начала Close.
	IS_CLOSING = errors.New("NatsBotEventSubscriber: остановка уже началась")
	// WORKERS_ALREADY_STARTED запрещает повторное создание пула воркеров.
	WORKERS_ALREADY_STARTED = errors.New("NatsBotEventSubscriber: workers уже запущены")
	// WORKERS_NOT_STARTED требует создать воркеры до вызова Start.
	WORKERS_NOT_STARTED = errors.New("NatsBotEventSubscriber: воркеры не запущены. Сначала CreateWorkers")
	// INVALID_WORKER_COUNT означает, что CreateWorkers получил Count <= 0.
	INVALID_WORKER_COUNT = errors.New("NatsBotEventSubscriber: неверное количество воркеров")
	// MESSAGE_QUEUE_NOT_CREATED означает, что CreateWorkers получил nil в поле MessagesQueue.
	MESSAGE_QUEUE_NOT_CREATED = errors.New("NatsBotEventSubscriber: MessageQueue не создана. Нужно создать в di")
	// MESSAGE_QUEUE_CLOSED означает, что callback пропустил сообщение после закрытия очереди.
	MESSAGE_QUEUE_CLOSED = errors.New("NatsBotEventSubscriber: очередь сообщений закрыта")
	// MESSAGE_TYPE_NOT_FOUND означает, что в subject отсутствует непустой суффикс типа события.
	MESSAGE_TYPE_NOT_FOUND = errors.New("NatsBotEventSubscriber: в subject отсутствует тип события после префикса")
	// SUBJECT_PREFIX_MISMATCH означает несовпадение префикса или недопустимый токен subject в его пределах.
	SUBJECT_PREFIX_MISMATCH = errors.New("NatsBotEventSubscriber: subject не соответствует префиксу подписки")
	// CANNOT_DECODE_MESSAGE обозначает отказ декодера; безопасная причина записывается отдельно.
	CANNOT_DECODE_MESSAGE = errors.New("NatsBotEventSubscriber: не удалось декодировать сообщение")
	// CANNOT_DISPATCH_EVENT объединяется в логе с ошибкой диспетчера, кроме отмены контекста.
	CANNOT_DISPATCH_EVENT = errors.New("NatsBotEventSubscriber: не удалось обработать событие")
	// CANNOT_ACK_MESSAGE объединяется в логе с ошибкой отправки Ack.
	CANNOT_ACK_MESSAGE = errors.New("NatsBotEventSubscriber: не удалось отправить подтверждение сообщения")
	// CANNOT_TERMINATE_MESSAGE объединяется в логе с ошибкой отправки Term.
	CANNOT_TERMINATE_MESSAGE = errors.New("NatsBotEventSubscriber: не удалось прекратить повторную доставку сообщения")
	// CLOSE_SUBSCRIPTION объединяется с ошибкой запуска Subscription.Drain.
	CLOSE_SUBSCRIPTION = errors.New("NatsBotEventSubscriber: остановка подписки NATS")
)

// NatsBotEventSubscriber принимает события ботов из JetStream и передаёт их
// декодеру и диспетчеру через общий пул воркеров. Реализует BotEventSubscriberPort;
// создаётся NatsBotEventSubscriberProvider, который задаёт зависимости, очередь
// и адресацию, затем вызывает CreateWorkers перед Start и Close при остановке Fx.
// Stream и consumer должны существовать заранее: Start использует nats.Bind.
//
// Экземпляр нельзя копировать после начала использования mutex и WaitGroup.
// До запуска нужны ненулевая MessagesQueue и зависимости обработки; Jetstream
// должен быть задан к вызову Start. Конфигурацию и зависимости нельзя менять
// конкурентно с использующими их методами. Очередь закрывает только Close.
// После начала Close повторный запуск этого экземпляра запрещён.
//
// Воркеры обрабатывают сообщения параллельно без гарантии порядка завершения.
// Подписчик не устраняет дубликаты; повторная доставка зависит от настроек consumer.
//
// Схема:
//
//	[JetStream: существующий consumer] -> [callback Start] -> MessagesQueue
//	                                                             |
//	                                                             v
//	[Ack при успехе] <- [Dispatcher] <- [Decoder] <- [воркеры HandleMessage]
//	Close -> [Drain подписки] -> [закрытие очереди] -> [ожидание воркеров]
type NatsBotEventSubscriber struct {
	Logger              *slog.Logger                 // Общий JSON-логгер из DI; обязателен для lifecycle и обработки сообщений.
	Subscription        *nats.Subscription           // Подписка, сохраняемая Start; nil до первой успешной подписки.
	Jetstream           nats.JetStreamContext        // Клиент JetStream, предоставляемый DI перед Start; соединением владеет внешний код.
	StreamName          string                       // Имя существующего stream для nats.Bind.
	SubjectPrefix       string                       // Префикс с точкой в конце; допускает токены *, Start добавляет >.
	ConsumerName        string                       // Имя существующего push consumer в StreamName.
	NatsBotEventDecoder ports.BotEventDecoderPort    // Преобразует тип и байты сообщения во внутреннее событие.
	BotEventDispatcher  ports.BotEventDispatcherPort // Передаёт внутреннее событие зарегистрированному обработчику.
	LifecycleMutex      sync.Mutex                   // Сериализует Start, CreateWorkers и инициализацию остановки в Close.
	WorkersWG           sync.WaitGroup               // Учитывает воркеров; Close ждёт их выхода после закрытия очереди.
	WorkersStarted      bool                         // Под LifecycleMutex отмечает создание пула; обратно в false не сбрасывается.
	MessageQueueMutex   sync.RWMutex                 // Защищает отправку callback в очередь от её одновременного закрытия.
	MessagesQueue       chan *nats.Msg               // Общая очередь из DI; воркеры читают её до закрытия и исчерпания сообщений.
	MessageQueueClosed  bool                         // Под MessageQueueMutex запрещает callback отправлять сообщения после закрытия очереди.
	Closing             bool                         // Под LifecycleMutex необратимо отмечает начало Close.
	CloseDone           chan struct{}                // Создаётся первым Close и закрывается после завершения фоновой очистки.
	CloseErr            error                        // Ошибка Drain; читается вызовами Close после сигнала CloseDone.
}

// Start подписывается на SubjectPrefix + ">" и направляет сообщения в MessagesQueue.
// Повторный вызов при действующей Subscription возвращает nil после проверок
// состояния; попытки повторить неудачную подписку выполняет DI-провайдер.
//
// Параметры:
//   - ctx — контекст вызывающего кода; здесь не проверяется и в Subscribe
//     не передаётся, поэтому не отменяет подписку или отправку в очередь.
//
// Результаты:
//   - error — IS_CLOSING, WORKERS_NOT_STARTED, NATS_NOT_CONNECTED либо
//     errors.Join(CANNOT_SUBSCRIBE, err) при ошибке Subscribe; nil при успехе.
//
// Побочные эффекты:
// Держит LifecycleMutex до возврата, сохраняет подписку и логирует сообщение
// об успешном запуске. Использует существующий consumer без его создания.
// ManualAck оставляет подтверждение HandleMessage. При заполненной очереди
// callback блокируется с RLock до освобождения места; закрытую очередь пропускает.
//
// Этапы:
//  1. Проверить состояние под LifecycleMutex и наличие действующей подписки.
//  2. Создать подписку с callback передачи сообщений и ручным подтверждением.
//  3. Сохранить подписку либо вернуть ошибку без её замены.
//
// Схема:
//
//	[Проверки] +-- ошибка -> error
//	          +-- действующая подписка -> nil
//	          +-- иначе -> [Subscribe с Bind и ManualAck] -> [сохранение] / error
//	callback -> [RLock, очередь закрыта?] +-- да -> выход без Ack
//	                                    +-- нет -> [отправка в очередь] -> RUnlock
func (NatsBotEventSubscriber *NatsBotEventSubscriber) Start(ctx context.Context) error {
	Started := time.Now()

	Tracer := otel.Tracer(
		"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/nats",
	)

	ctx, span := Tracer.Start(ctx, "NatsBotEventSubscriber.Start")
	defer span.End()

	Logger := NatsBotEventSubscriber.Logger.With(
		"operation", "start_bot_event_subscriber",
	)

	Logger.DebugContext(ctx, "Проверка состояния перед запуском подписки")
	// Этап 1: проверки и создание подписки не должны пересекаться с началом Close.
	NatsBotEventSubscriber.LifecycleMutex.Lock()
	defer NatsBotEventSubscriber.LifecycleMutex.Unlock()

	if NatsBotEventSubscriber.Closing {
		Logger.DebugContext(
			ctx, "Запуск подписки отклонён",
			"error", IS_CLOSING,
			"duration_ms", time.Since(Started).Milliseconds(),
		)
		span.RecordError(IS_CLOSING)
		span.SetStatus(codes.Error, IS_CLOSING.Error())
		return IS_CLOSING
	}
	if !NatsBotEventSubscriber.WorkersStarted {
		Logger.DebugContext(
			ctx, "Запуск подписки отклонён",
			"error", WORKERS_NOT_STARTED,
			"duration_ms", time.Since(Started).Milliseconds(),
		)
		span.RecordError(WORKERS_NOT_STARTED)
		span.SetStatus(codes.Error, WORKERS_NOT_STARTED.Error())
		return WORKERS_NOT_STARTED
	}
	if NatsBotEventSubscriber.Jetstream == nil {
		Logger.DebugContext(
			ctx, "Запуск подписки отклонён",
			"error", NATS_NOT_CONNECTED,
			"duration_ms", time.Since(Started).Milliseconds(),
		)
		span.RecordError(NATS_NOT_CONNECTED)
		span.SetStatus(codes.Error, NATS_NOT_CONNECTED.Error())
		return NATS_NOT_CONNECTED
	}
	if NatsBotEventSubscriber.Subscription != nil && NatsBotEventSubscriber.Subscription.IsValid() {
		Logger.DebugContext(
			ctx, "Подписка уже действует",
			"duration_ms", time.Since(Started).Milliseconds(),
		)
		return nil
	}

	// Этап 2: callback передаёт транспортное сообщение воркерам без декодирования.
	Subject := NatsBotEventSubscriber.SubjectPrefix + ">"
	Logger.DebugContext(
		ctx, "Создание подписки JetStream",
		"subject", Subject,
	)

	MessageQueue := NatsBotEventSubscriber.MessagesQueue
	Subscription, err := NatsBotEventSubscriber.Jetstream.Subscribe(
		Subject,
		func(Message *nats.Msg) {
			// RLock удерживается и при ожидании места: Close не закроет канал под отправителем.
			NatsBotEventSubscriber.MessageQueueMutex.RLock()
			defer NatsBotEventSubscriber.MessageQueueMutex.RUnlock()
			if NatsBotEventSubscriber.MessageQueueClosed {
				NatsBotEventSubscriber.Logger.DebugContext(
					ctx, "Сообщение пропущено",
					"error", MESSAGE_QUEUE_CLOSED,
					"operation", "enqueue_bot_event",
					"subject", Message.Subject,
					"duration_ms", time.Since(Started).Milliseconds(),
				)
				return
			}
			NatsBotEventSubscriber.Logger.DebugContext(
				ctx, "Передача сообщения в очередь",
				"operation", "enqueue_bot_event",
				"subject", Message.Subject,
				// ! "message_id", Message.Header.Get(nats.MsgIdHdr),
			)
			MessageQueue <- Message
		},
		nats.Bind(NatsBotEventSubscriber.StreamName, NatsBotEventSubscriber.ConsumerName),
		nats.ManualAck(),
	)

	// Этап 3: неудачная попытка сохраняет состояние для повторного Start.
	if err != nil {
		err = errors.Join(CANNOT_SUBSCRIBE, err)
		// Итоговую ошибку попытки пишет DI-провайдер, управляющий повторами.
		Logger.DebugContext(
			ctx, "Попытка создания подписки завершилась ошибкой",
			"error", err,
			"duration_ms", time.Since(Started).Milliseconds(),
		)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	NatsBotEventSubscriber.Subscription = Subscription
	Logger.InfoContext(
		ctx, "Подписчик событий ботов запущен",
		"subject", Subject,
		"duration_ms", time.Since(Started).Milliseconds(),
	)

	return nil
}

// HandleMessage последовательно извлекает тип, декодирует событие, вызывает
// диспетчер и подтверждает сообщение только после успешного завершения этих шагов.
//
// Параметры:
//   - ctx — контекст из CreateWorkers или прямого вызова; передаётся разбору,
//     декодеру и диспетчеру. Сам метод отмену не проверяет; поддержка зависит
//     от зависимостей. В Message.Ack контекст не передаётся.
//   - Message — ненулевое сообщение JetStream: Subject определяет маршрут,
//     Data передаётся декодеру без изменений, Ack отправляет подтверждение.
//
// Побочные эффекты:
// Вызывает обработчик через BotEventDispatcher и логирует итог обработки,
// не возвращая ошибки воркеру. При ошибке разбора subject или диспетчера
// выходит без Ack, Nak и Term. При ошибке декодирования вызывает Term,
// кроме context.Canceled, включая обёрнутую ошибку.
// При AckExplicit дальнейшая доставка неподтверждённого сообщения зависит
// от AckWait, BackOff, MaxDeliver и состояния consumer.
// Ack вызывается без ожидания подтверждения сервера; его ошибка только
// логируется. Дедупликации и отката побочных эффектов обработчика
// при ошибке Ack нет.
//
// Этапы:
//  1. Извлечь ключ маршрута из Subject.
//  2. Передать ключ и Data декодеру для получения внутреннего события.
//  3. Передать событие диспетчеру и дождаться результата обработчика.
//  4. Отправить Ack после успешной обработки.
//
// Схема:
//
//	Subject -> [GetMessageType] --ошибка--> [лог, выход]
//	                |
//	                v
//	[BotEventDecode с Data] --ошибка--> [Canceled: лог, выход; иначе: лог, Term]
//	                |
//	                v
//	[BotEventDispatch] --ошибка--> [лог, выход без подтверждения]
//	                |
//	                v
//	[Ack] -> [ERROR при ошибке / INFO при успехе]
func (NatsBotEventSubscriber *NatsBotEventSubscriber) HandleMessage(
	ctx context.Context,
	Message *nats.Msg,
) {
	Started := time.Now()

	Tracer := otel.Tracer(
		"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/nats",
	)

	ctx, span := Tracer.Start(ctx, "NatsBotEventSubscriber.HandleMessage")
	defer span.End()

	Logger := NatsBotEventSubscriber.Logger.With(
		"operation", "handle_bot_event",
		"subject", Message.Subject,
		//  ! "message_id", Message.Header.Get(nats.MsgIdHdr),
	)
	Logger.DebugContext(
		ctx, "Сообщение получено воркером",
		"payload_size", len(Message.Data),
	)

	// Этап 1: subject определяет транспортный тип вместе с версией контракта.
	Logger.DebugContext(ctx, "Определение типа сообщения по subject")
	MessageType, err := NatsBotEventSubscriber.GetMessageType(
		ctx,
		Message.Subject,
	)
	if err != nil {
		Logger.WarnContext(
			ctx, "Сообщение отклонено",
			"error", err,
			"duration_ms", time.Since(Started).Milliseconds(),
		)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return
	}
	Logger = Logger.With("message_type", MessageType)

	// Этап 2: декодер отвечает за разбор контракта и создание внутреннего события.
	Logger.DebugContext(ctx, "Передача сообщения декодеру")
	BotEvent, err := NatsBotEventSubscriber.NatsBotEventDecoder.BotEventDecode(
		ctx,
		MessageType, Message.Data,
	)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			Logger.DebugContext(
				ctx, "Декодирование сообщения отменено",
				"error", context.Canceled,
				"duration_ms", time.Since(Started).Milliseconds(),
			)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			Logger.WarnContext(
				ctx, "Сообщение отклонено при декодировании",
				"error", CANNOT_DECODE_MESSAGE,
				"cause", botEventDecodeErrorLogValue(err),
				"duration_ms", time.Since(Started).Milliseconds(),
			)
			span.RecordError(CANNOT_DECODE_MESSAGE)
			span.SetStatus(codes.Error, CANNOT_DECODE_MESSAGE.Error())
			if termErr := Message.Term(); termErr != nil { // Term сообщения
				err := errors.Join(CANNOT_TERMINATE_MESSAGE, termErr)
				Logger.ErrorContext(
					ctx, "Отправка Term завершилась ошибкой",
					"error", err,
				)
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
		}
		return
	}
	Logger = Logger.With("event_type", fmt.Sprintf("%T", BotEvent))

	// Этап 3: диспетчер выбирает обработчик по конкретному Go-типу события.
	Logger.DebugContext(ctx, "Передача события диспетчеру")
	err = NatsBotEventSubscriber.BotEventDispatcher.BotEventDispatch(
		ctx,
		BotEvent,
	)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			Logger.DebugContext(
				ctx, "Обработка события отменена",
				"error", err,
				"duration_ms", time.Since(Started).Milliseconds(),
			)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			err := errors.Join(CANNOT_DISPATCH_EVENT, err)
			Logger.ErrorContext(
				ctx, "Обработка события завершилась ошибкой",
				"error", err, 
				"duration_ms", time.Since(Started).Milliseconds(),
			)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return
	}

	// Этап 4: успешная передача в очередь сама по себе не подтверждает обработку.
	Logger.DebugContext(ctx, "Отправка подтверждения сообщения")
	if err := Message.Ack(); err != nil {
		err := errors.Join(CANNOT_ACK_MESSAGE, err)
		Logger.ErrorContext(
			ctx, "Отправка Ack завершилась ошибкой",
			"error", err, 
			"duration_ms", time.Since(Started).Milliseconds(),
		)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		Logger.InfoContext(
			ctx, "Сообщение обработано, Ack отправлен",
			"duration_ms", time.Since(Started).Milliseconds(),
		)
	}
}

// GetMessageType возвращает тип события после префикса подписки.
// В SubjectPrefix токен * соответствует одному непустому токену subject,
// остальные токены сопоставляются буквально. Завершающая точка префикса
// при разборе необязательна; суффикс типа возвращается без проверки его токенов.
//
// Параметры:
//   - ctx — контекст вызывающего кода; локальный разбор не использует отмену.
//   - Subject — конкретный subject сообщения, например
//     bot.bot-1.to.control.event.v1.lifecycle.bot_started_event.
//
// Результаты:
//   - string — суффикс типа с сохранением точек, например
//     v1.lifecycle.bot_started_event; при ошибке пустая строка.
//   - error — обёртка MESSAGE_TYPE_NOT_FOUND при отсутствии непустого суффикса
//     либо SUBJECT_PREFIX_MISMATCH при несовпадении префикса, пустом токене
//     или символах * и > в части Subject, соответствующей префиксу.
//
// Этапы:
//  1. Разделить префикс и subject, оставив суффикс типа целиком.
//  2. Сопоставить токены префикса и вернуть суффикс либо ошибку.
//
// Схема:
//
//	Subject -> [Разделение] +-- нет суффикса -> error
//	                       +-- есть -> [Сопоставление префикса с *] -> тип / error
func (NatsBotEventSubscriber *NatsBotEventSubscriber) GetMessageType(
	ctx context.Context,
	Subject string,
) (string, error) {

	Tracer := otel.Tracer(
		"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/nats",
	)
	ctx, span := Tracer.Start(ctx, "NatsBotEventSubscriber.GetMessageType")
	defer span.End()

	// Этап 1: ограничение SplitN сохраняет тип с точками последним элементом.
	PrefixTokens := strings.Split(strings.TrimSuffix(NatsBotEventSubscriber.SubjectPrefix, "."), ".")
	SubjectTokens := strings.SplitN(Subject, ".", len(PrefixTokens)+1)
	if len(SubjectTokens) != len(PrefixTokens)+1 || SubjectTokens[len(PrefixTokens)] == "" {
		span.RecordError(MESSAGE_TYPE_NOT_FOUND)
		span.SetStatus(codes.Error, MESSAGE_TYPE_NOT_FOUND.Error())
		return "", fmt.Errorf("%w: subject %q", MESSAGE_TYPE_NOT_FOUND, Subject)
	}

	// Этап 2: wildcard занимает один конкретный токен; * и > во входном токене запрещены.
	for Index, PrefixToken := range PrefixTokens {
		Token := SubjectTokens[Index]
		if Token == "" || strings.ContainsAny(Token, "*>") ||
			(PrefixToken != "*" && PrefixToken != Token) {
			span.RecordError(SUBJECT_PREFIX_MISMATCH)
			span.SetStatus(codes.Error, SUBJECT_PREFIX_MISMATCH.Error())
			return "", fmt.Errorf("%w: subject %q, префикс %q", SUBJECT_PREFIX_MISMATCH, Subject, NatsBotEventSubscriber.SubjectPrefix)
		}
	}
	return SubjectTokens[len(PrefixTokens)], nil
}

// CreateWorkers однократно запускает пул чтения MessagesQueue до создания подписки.
// Каждый воркер синхронно вызывает HandleMessage для очередного сообщения;
// между воркерами обработка выполняется параллельно.
//
// Параметры:
//   - ctx — общий контекст, передаваемый каждому HandleMessage. Его отмена
//     сама по себе не прерывает чтение канала и не завершает воркеры.
//   - Count — положительное число воркеров, создаваемых этим вызовом.
//
// Результаты:
//   - error — INVALID_WORKER_COUNT при Count <= 0, IS_CLOSING после начала Close,
//     WORKERS_ALREADY_STARTED при повторном запуске, MESSAGE_QUEUE_NOT_CREATED
//     при nil в MessagesQueue; nil после запуска goroutine без ожидания их готовности.
//
// Побочные эффекты:
// Под LifecycleMutex устанавливает WorkersStarted и регистрирует Count воркеров
// в WorkersWG до их запуска. Воркеры завершаются после закрытия и исчерпания
// очереди в Close. Ошибки обработки сообщений не прекращают чтение очереди.
//
// Этапы:
//  1. Проверить Count, затем состояние подписчика под LifecycleMutex.
//  2. Зарегистрировать пул в WorkersWG и отметить его запуск.
//  3. Запустить Count goroutine, читающих общую очередь до её исчерпания.
//
// Схема:
//
//	[Проверки] +-- ошибка -> error
//	          +-- успех -> [регистрация пула] -> [запуск Count goroutine] -> nil
//	каждый воркер: [чтение очереди] +-- сообщение -> HandleMessage -> повтор
//	                               +-- закрыта и пуста -> WorkersWG.Done
func (NatsBotEventSubscriber *NatsBotEventSubscriber) CreateWorkers(
	ctx context.Context,
	Count int,
) error {

	WorkerCtx := ctx // Сохраняем исходный ctx, чтобы сообщения не наследовали общий span запуска воркеров.

	Tracer := otel.Tracer(
		"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/nats",
	)

	ctx, span := Tracer.Start(ctx, "NatsBotEventSubscriber.CreateWorkers")
	defer span.End()

	Logger := NatsBotEventSubscriber.Logger.With(
		"operation", "create_bot_event_workers",
		"worker_count", Count,
	)

	Logger.DebugContext(ctx, "Проверка состояния перед запуском воркеров")
	// Этап 1: проверка Count не требует блокировки, состояние пула защищает mutex.
	if Count <= 0 {
		Logger.ErrorContext(ctx, "Неверное количество воркеров", "error", INVALID_WORKER_COUNT)
		span.RecordError(INVALID_WORKER_COUNT)
		span.SetStatus(codes.Error, INVALID_WORKER_COUNT.Error())
		return INVALID_WORKER_COUNT
	}

	NatsBotEventSubscriber.LifecycleMutex.Lock()
	defer NatsBotEventSubscriber.LifecycleMutex.Unlock()

	if NatsBotEventSubscriber.Closing {
		Logger.DebugContext(
			ctx, "Запуск воркеров отклонён",
			"error", IS_CLOSING,
		)
		span.RecordError(IS_CLOSING)
		span.SetStatus(codes.Error, IS_CLOSING.Error())
		return IS_CLOSING
	}
	if NatsBotEventSubscriber.WorkersStarted {
		Logger.ErrorContext(
			ctx, "Повторный запуск воркеров отклонён",
			"error", WORKERS_ALREADY_STARTED,
		)
		span.RecordError(WORKERS_ALREADY_STARTED)
		span.SetStatus(codes.Error, WORKERS_ALREADY_STARTED.Error())
		return WORKERS_ALREADY_STARTED
	}
	if NatsBotEventSubscriber.MessagesQueue == nil {
		Logger.ErrorContext(
			ctx, "Очередь сообщений не создана",
			"error", MESSAGE_QUEUE_NOT_CREATED,
		)
		span.RecordError(MESSAGE_QUEUE_NOT_CREATED)
		span.SetStatus(codes.Error, MESSAGE_QUEUE_NOT_CREATED.Error())
		return MESSAGE_QUEUE_NOT_CREATED
	}

	// Этап 2: Wait в Close должен увидеть весь пул ещё до запуска goroutine.
	MessageQueue := NatsBotEventSubscriber.MessagesQueue
	NatsBotEventSubscriber.WorkersWG.Add(Count)
	NatsBotEventSubscriber.WorkersStarted = true

	// Этап 3: все воркеры используют одну очередь и один контекст обработки.
	Logger.DebugContext(
		ctx, "Запуск пула воркеров",
		"queue_capacity", cap(MessageQueue),
	)
	for i := 0; i < Count; i++ {
		go func() {
			defer NatsBotEventSubscriber.WorkersWG.Done()

			for Message := range MessageQueue {
				NatsBotEventSubscriber.HandleMessage(WorkerCtx, Message)
			}
		}()
	}

	Logger.InfoContext(ctx, "Пул воркеров обработки событий запущен")
	return nil
}

// Close однократно начинает фоновую остановку подписки и воркеров, затем ждёт
// её завершения. Повторные и конкурентные вызовы ждут общий CloseDone;
// после начала остановки Start и CreateWorkers возвращают IS_CLOSING.
//
// Параметры:
//   - ctx — ненулевой контекст ожидания этого вызова. Его отмена позволяет
//     вернуть ctx.Err(), но не отменяет Drain, чтение очереди или обработчики
//     и не останавливает фоновую очистку. Воркеры сохраняют контекст CreateWorkers.
//
// Результаты:
//   - error — CloseErr после завершения очистки: nil либо
//     errors.Join(CLOSE_SUBSCRIPTION, err) при ошибке запуска Drain;
//     ctx.Err(), если выбрана ветка отмены ожидания. При одновременной готовности
//     обеих веток select результат может быть любым из этих двух.
//
// Побочные эффекты:
// Отмечает Closing, создаёт CloseDone и запускает одну goroutine очистки.
// При успешном Drain ждёт SubscriptionClosed; при ошибке сохраняет её
// и продолжает закрытие очереди без этого ожидания. Под MessageQueueMutex
// запрещает новые отправки, закрывает ненулевую очередь и ждёт всех воркеров.
// Соединение JetStream не закрывает. Без отмены ctx ожидание может длиться
// неограниченно, в том числе при зависшем обработчике или отправителе в очередь.
//
// Этапы:
//  1. Под LifecycleMutex однократно подготовить и запустить очистку.
//  2. В фоне вызвать Drain и при успехе дождаться закрытия подписки.
//  3. В фоне запретить отправки и закрыть очередь под исключительной блокировкой.
//  4. В фоне дождаться воркеров и закрыть CloseDone.
//  5. Освободить LifecycleMutex и ждать CloseDone либо отмены ctx.
//
// Схема:
//
//	Close -> [первая остановка?] +-- да -> [Closing, CloseDone, запуск очистки]
//	                            +-- нет -> [использовать общий CloseDone]
//	очистка -> [подписка задана?] +-- нет -> [закрытие очереди]
//	                            +-- да -> [Drain] +-- ошибка -> [CloseErr]
//	                                             +-- успех -> [SubscriptionClosed]
//	[CloseErr / SubscriptionClosed] -> [закрытие очереди]
//	[закрытие очереди] -> [WorkersWG.Wait] -> [закрытие CloseDone]
//	вызов Close -> [ожидание без LifecycleMutex] +-- CloseDone -> CloseErr
//	                                            +-- ctx.Done -> ctx.Err()
func (NatsBotEventSubscriber *NatsBotEventSubscriber) Close(
	ctx context.Context,
) error {
	Started := time.Now()

	Tracer := otel.Tracer(
		"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/nats",
	)

	ctx, span := Tracer.Start(ctx, "NatsBotEventSubscriber.Close")
	defer span.End()

	Logger := NatsBotEventSubscriber.Logger.With(
		"operation", "close_bot_event_subscriber",
	)

	Logger.DebugContext(ctx, "Запрошена остановка подписчика")
	// Этап 1: все вызовы получают общий сигнал, а очистка запускается только один раз.
	NatsBotEventSubscriber.LifecycleMutex.Lock()

	if !NatsBotEventSubscriber.Closing {
		NatsBotEventSubscriber.Closing = true
		NatsBotEventSubscriber.CloseDone = make(chan struct{})
		Subscription := NatsBotEventSubscriber.Subscription
		MessagesQueue := NatsBotEventSubscriber.MessagesQueue
		Started := time.Now()

		go func() {
			// Этап 4: сигнал завершения публикует CloseErr после выхода всех воркеров.
			defer close(NatsBotEventSubscriber.CloseDone)

			// Этап 2: подписываемся на статус до Drain, чтобы дождаться окончания callback.
			if Subscription != nil {
				Logger.DebugContext(ctx, "Остановка приёма сообщений через Drain")
				Closed := NatsBotEventSubscriber.Subscription.StatusChanged(nats.SubscriptionClosed)
				if err := Subscription.Drain(); err != nil {
					NatsBotEventSubscriber.CloseErr = errors.Join(CLOSE_SUBSCRIPTION, err)
					Logger.DebugContext(
						ctx, "Drain завершился ошибкой; остановка воркеров продолжается",
						"error", NatsBotEventSubscriber.CloseErr,
					)
				} else {
					<-Closed
					Logger.DebugContext(ctx, "Подписка JetStream закрыта")
				}
			}

			// Этап 3: ждём текущих отправителей; будущие callback увидят запрет отправки.
			Logger.DebugContext(ctx, "Закрытие очереди сообщений")
			NatsBotEventSubscriber.MessageQueueMutex.Lock()
			NatsBotEventSubscriber.MessageQueueClosed = true

			if MessagesQueue != nil {
				close(MessagesQueue)
			}
			NatsBotEventSubscriber.MessageQueueMutex.Unlock()
			// Этап 4: range в воркерах дочитает буфер и завершится после закрытия канала.
			Logger.DebugContext(ctx, "Ожидание завершения воркеров")
			NatsBotEventSubscriber.WorkersWG.Wait()
			// Очистка владеет итогом даже после отмены ожидающего вызова Close.
			if NatsBotEventSubscriber.CloseErr != nil {
				Logger.ErrorContext(ctx, "Остановка подписчика завершена с ошибкой",
					"error", NatsBotEventSubscriber.CloseErr, "duration_ms", time.Since(Started).Milliseconds(),
				)
			} else {
				Logger.InfoContext(ctx, "Подписчик событий ботов остановлен",
					"duration_ms", time.Since(Started).Milliseconds(),
				)
			}
		}()
	} else {
		Logger.DebugContext(ctx, "Остановка уже запрошена; ожидание общей очистки")
	}

	// Этап 5: ожидание не удерживает mutex и не блокирует другие вызовы Close.
	CloseDone := NatsBotEventSubscriber.CloseDone
	NatsBotEventSubscriber.LifecycleMutex.Unlock()

	select {
	case <-CloseDone:
		err := NatsBotEventSubscriber.CloseErr
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return err
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.Canceled) {
			Logger.DebugContext(
				ctx, "Ожидание остановки отменено; очистка продолжается",
				"error", ctx.Err(),
				"duration_ms", time.Since(Started).Milliseconds(),
			)
			span.RecordError(ctx.Err())
			span.SetStatus(codes.Error, ctx.Err().Error())
		} else {
			Logger.ErrorContext(
				ctx, "Истёк срок ожидания остановки; очистка продолжается",
				"error", ctx.Err(),
				"duration_ms", time.Since(Started).Milliseconds(),
			)
			span.RecordError(ctx.Err())
			span.SetStatus(codes.Error, ctx.Err().Error())
		}
		return ctx.Err()
	}
}
