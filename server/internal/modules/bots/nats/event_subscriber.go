package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/ports"
	"github.com/nats-io/nats.go"
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
// Держит LifecycleMutex до возврата, сохраняет подписку и печатает сообщение
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
	// Этап 1: проверки и создание подписки не должны пересекаться с началом Close.
	NatsBotEventSubscriber.LifecycleMutex.Lock()
	defer NatsBotEventSubscriber.LifecycleMutex.Unlock()

	if NatsBotEventSubscriber.Closing {
		return IS_CLOSING
	}
	if !NatsBotEventSubscriber.WorkersStarted {
		return WORKERS_NOT_STARTED
	}
	if NatsBotEventSubscriber.Jetstream == nil {
		return NATS_NOT_CONNECTED
	}
	if NatsBotEventSubscriber.Subscription != nil && NatsBotEventSubscriber.Subscription.IsValid() {
		return nil
	}

	// Этап 2: callback передаёт транспортное сообщение воркерам без декодирования.
	Subject := NatsBotEventSubscriber.SubjectPrefix + ">"

	MessageQueue := NatsBotEventSubscriber.MessagesQueue
	Subscription, err := NatsBotEventSubscriber.Jetstream.Subscribe(
		Subject,
		func(Message *nats.Msg) {
			// RLock удерживается и при ожидании места: Close не закроет канал под отправителем.
			NatsBotEventSubscriber.MessageQueueMutex.RLock()
			defer NatsBotEventSubscriber.MessageQueueMutex.RUnlock()
			if NatsBotEventSubscriber.MessageQueueClosed {
				return
			}
			MessageQueue <- Message
		},
		nats.Bind(NatsBotEventSubscriber.StreamName, NatsBotEventSubscriber.ConsumerName),
		nats.ManualAck(),
	)

	// Этап 3: неудачная попытка сохраняет состояние для повторного Start.
	if err != nil {
		return errors.Join(CANNOT_SUBSCRIBE, err)
	}

	NatsBotEventSubscriber.Subscription = Subscription
	fmt.Println("NatsBotEventSubscriber успешно запустился")

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
// Вызывает обработчик через BotEventDispatcher и печатает ошибки в stdout,
// не возвращая их воркеру. При ошибке до Ack выходит без Ack, Nak или Term;
// дальнейшую доставку определяет consumer. Ack вызывается без ожидания
// подтверждения сервера; его ошибка только печатается. Дедупликации и отката
// побочных эффектов обработчика при ошибке Ack нет.
//
// Этапы:
//  1. Извлечь ключ маршрута из Subject.
//  2. Передать ключ и Data декодеру для получения внутреннего события.
//  3. Передать событие диспетчеру и дождаться результата обработчика.
//  4. Отправить Ack после успешной обработки.
//
// Схема:
//
//	Subject -> [GetMessageType] -> [BotEventDecode с Data] -> [BotEventDispatch]
//	                 |                      |                       |
//	               ошибка                 ошибка                  ошибка
//	                 v                      v                       v
//	            [печать, выход без Ack при любой из этих ошибок]
//	[успех BotEventDispatch] -> [Ack] -> [при ошибке печать] -> выход
func (NatsBotEventSubscriber *NatsBotEventSubscriber) HandleMessage(
	ctx context.Context,
	Message *nats.Msg,
) {
	// Этап 1: subject определяет транспортный тип вместе с версией контракта.
	MessageType, err := NatsBotEventSubscriber.GetMessageType(
		ctx, Message.Subject,
	)
	if err != nil {
		fmt.Printf("ошибка определения типа сообщения: %v\n", err)
		return
	}

	// Этап 2: декодер отвечает за разбор контракта и создание внутреннего события.
	BotEvent, err := NatsBotEventSubscriber.NatsBotEventDecoder.BotEventDecode(
		ctx, MessageType, Message.Data,
	)
	if err != nil {
		fmt.Printf("ошибка декодирования: %v\n", err)
		return
	}

	// Этап 3: диспетчер выбирает обработчик по конкретному Go-типу события.
	err = NatsBotEventSubscriber.BotEventDispatcher.BotEventDispatch(
		ctx, BotEvent,
	)
	if err != nil {
		fmt.Printf("ошибка обработки события: %v\n", err)
		return
	}

	// Этап 4: успешная передача в очередь сама по себе не подтверждает обработку.
	if err := Message.Ack(); err != nil {
		fmt.Printf("ошибка подтверждения сообщения: %v\n", err)
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
//   - error — отсутствие непустого суффикса, несовпадение префикса либо
//     пустой токен или символы * и > в части Subject, соответствующей префиксу.
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
	// Этап 1: ограничение SplitN сохраняет тип с точками последним элементом.
	PrefixTokens := strings.Split(strings.TrimSuffix(NatsBotEventSubscriber.SubjectPrefix, "."), ".")
	SubjectTokens := strings.SplitN(Subject, ".", len(PrefixTokens)+1)
	if len(SubjectTokens) != len(PrefixTokens)+1 || SubjectTokens[len(PrefixTokens)] == "" {
		return "", fmt.Errorf("в subject %q отсутствует тип события после префикса", Subject)
	}

	// Этап 2: wildcard занимает один конкретный токен; * и > во входном токене запрещены.
	for Index, PrefixToken := range PrefixTokens {
		Token := SubjectTokens[Index]
		if Token == "" || strings.ContainsAny(Token, "*>") ||
			(PrefixToken != "*" && PrefixToken != Token) {
			return "", fmt.Errorf("subject %q не соответствует префиксу %q", Subject, NatsBotEventSubscriber.SubjectPrefix)
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
	// Этап 1: проверка Count не требует блокировки, состояние пула защищает mutex.
	if Count <= 0 {
		return INVALID_WORKER_COUNT
	}

	NatsBotEventSubscriber.LifecycleMutex.Lock()
	defer NatsBotEventSubscriber.LifecycleMutex.Unlock()

	if NatsBotEventSubscriber.Closing {
		return IS_CLOSING
	}
	if NatsBotEventSubscriber.WorkersStarted {
		return WORKERS_ALREADY_STARTED
	}
	if NatsBotEventSubscriber.MessagesQueue == nil {
		return MESSAGE_QUEUE_NOT_CREATED
	}

	// Этап 2: Wait в Close должен увидеть весь пул ещё до запуска goroutine.
	MessageQueue := NatsBotEventSubscriber.MessagesQueue
	NatsBotEventSubscriber.WorkersWG.Add(Count)
	NatsBotEventSubscriber.WorkersStarted = true

	// Этап 3: все воркеры используют одну очередь и один контекст обработки.
	for i := 0; i < Count; i++ {
		go func() {
			defer NatsBotEventSubscriber.WorkersWG.Done()

			for Message := range MessageQueue {
				NatsBotEventSubscriber.HandleMessage(ctx, Message)
			}
		}()
	}

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
	// Этап 1: все вызовы получают общий сигнал, а очистка запускается только один раз.
	NatsBotEventSubscriber.LifecycleMutex.Lock()

	if !NatsBotEventSubscriber.Closing {
		NatsBotEventSubscriber.Closing = true
		NatsBotEventSubscriber.CloseDone = make(chan struct{})
		Subscription := NatsBotEventSubscriber.Subscription
		MessagesQueue := NatsBotEventSubscriber.MessagesQueue

		go func() {
			// Этап 4: сигнал завершения публикует CloseErr после выхода всех воркеров.
			defer close(NatsBotEventSubscriber.CloseDone)

			// Этап 2: подписываемся на статус до Drain, чтобы дождаться окончания callback.
			if Subscription != nil {
				Closed := NatsBotEventSubscriber.Subscription.StatusChanged(nats.SubscriptionClosed)
				if err := Subscription.Drain(); err != nil {
					NatsBotEventSubscriber.CloseErr = errors.Join(CLOSE_SUBSCRIPTION, err)
				} else {
					<-Closed
				}
			}

			// Этап 3: ждём текущих отправителей; будущие callback увидят запрет отправки.
			NatsBotEventSubscriber.MessageQueueMutex.Lock()
			NatsBotEventSubscriber.MessageQueueClosed = true

			if MessagesQueue != nil {
				close(MessagesQueue)
			}
			NatsBotEventSubscriber.MessageQueueMutex.Unlock()
			// Этап 4: range в воркерах дочитает буфер и завершится после закрытия канала.
			NatsBotEventSubscriber.WorkersWG.Wait()
		}()
	}

	// Этап 5: ожидание не удерживает mutex и не блокирует другие вызовы Close.
	CloseDone := NatsBotEventSubscriber.CloseDone
	NatsBotEventSubscriber.LifecycleMutex.Unlock()

	select {
	case <-CloseDone:
		return NatsBotEventSubscriber.CloseErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
