package di_bots

import (
	"context"
	"log"
	"time"

	bots_nats "github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/nats"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/ports"
	"github.com/nats-io/nats.go"
	"go.uber.org/fx"
)

const (
	NATS_BOT_EVENT_STREAM         = "BOT_TO_CONTROL_EVENTS"              // Существующий stream событий ботов, к которому Start привязывает подписку через nats.Bind.
	NATS_BOT_EVENT_CONSUMER       = "control_plane_bot_event_subscriber" // Существующий consumer в stream; провайдер не создаёт его.
	NATS_BOT_EVENT_SUBJECT_PREFIX = "bot.*.to.control.event."            // * занимает токен ID бота; Start добавляет > для суффикса типа события.

	BOT_EVENT_WORKERS_COUNT = 4               // Число goroutine, обрабатывающих сообщения из общей очереди.
	BOT_EVENT_RETRY_DELAY   = 2 * time.Second // Пауза после ошибки получения JetStream или создания подписки.
)

// NatsBotEventSubscriberProvider собирает NATS-подписчика событий ботов
// и передаёт управление его запуском и остановкой lifecycle Fx.
// В main результат востребован через fx.Invoke, поэтому Fx вызывает провайдер
// и регистрирует хуки даже без прямых вызовов методов возвращённого порта.
//
// Параметры:
//   - lc — lifecycle Fx для регистрации хуков; должен быть задан. В текущем
//     графе зависимостей хук остановки подписчика вызывается до хука NatsClient.
//   - GetJetStream — обязательная фабрика отложенного получения JetStream.
//     NatsBotEventJetStreamProvider ждёт общего соединения через WaitConnection;
//     переданный фабрике SubscribeCtx отменяет это ожидание.
//   - Decoder — обязательный декодер: по типу из subject преобразует байты
//     сообщения во внутреннее событие; вызывается воркерами подписчика.
//   - Dispatcher — обязательный диспетчер: передаёт внутреннее событие
//     зарегистрированному обработчику; вызывается после успешного декодирования.
//
// Результаты:
//   - BotEventSubscriberPort — указатель на NatsBotEventSubscriber с адресацией
//     из констант и буфером на bots_nats.MESSAGES_QUEUE_LIMIT сообщений.
//     На момент возврата воркеры и подписка ещё не запущены, Jetstream не задан.
//
// Побочные эффекты и контракт:
// Сам провайдер создаёт очередь и регистрирует хуки, не подключаясь к NATS.
// Воркеры вызывают Decoder и Dispatcher конкурентно; порядок завершения
// обработки сообщений не гарантируется.
// OnStart игнорирует контекст запуска Fx: RunCtx создаётся от context.Background,
// а SubscribeCtx — от RunCtx. После запуска воркеров OnStart возвращает nil,
// не дожидаясь готовности подписки. Ошибка CreateWorkers возвращается Fx
// с отменой обоих контекстов; ошибки фабрики и Start логируются и повторяются
// без ограничения числа попыток до успеха либо отмены SubscribeCtx.
// После успешного Start цикл завершается и больше не наблюдает за подпиской.
// Start не передаёт SubscribeCtx в JetStream.Subscribe, поэтому отмена
// контекста не прерывает уже выполняющийся вызов Subscribe напрямую.
//
// OnStop отменяет попытки, ждёт завершения цикла и вызывает Close(ctx).
// Close запускает Drain подписки, закрывает очередь и ждёт воркеры; хук
// возвращает его результат либо ctx.Err(), если stop-контекст завершился раньше.
// После таймаута хука Close всё равно будет вызван, когда завершится цикл;
// его фоновая очистка также может продолжаться после возврата хука.
// При любом выходе из OnStop отменяется RunCtx, передаваемый обработке сообщений.
// Сама отмена RunCtx не завершает чтение очереди: воркеры выходят после её закрытия.
// Провайдер не закрывает общее соединение NATS; им владеет NatsClient.
//
// Этапы:
//  1. Создать подписчика с зависимостями и очередью, сохранить состояние хуков.
//  2. Зарегистрировать OnStart: создать контексты, запустить воркеры и цикл подписки.
//  3. В цикле получить JetStream и вызвать Start; при ошибке выдержать паузу.
//  4. Зарегистрировать OnStop: отменить цикл, дождаться его выхода и вызвать Close.
//
// Схема:
//
//	[Провайдер] -> [Подписчик, очередь, хуки] -> BotEventSubscriberPort
//	OnStart -> [RunCtx, SubscribeCtx] -> [CreateWorkers]
//	                                     +-- ошибка -> отмена контекстов -> error
//	                                     +-- успех -> [Запуск цикла в goroutine] -> nil
//	[Цикл] -> [GetJetStream] -> [Start]
//	            |                 +-- успех -> выход из цикла
//	            +-- ошибка -------+-- ошибка -> [Лог, пауза 2 с] -> повтор
//	            Отмена SubscribeCtx -> выход после текущей операции / ожидания
//	OnStop -> [CancelSubscribe] -> [Ожидание SubscribeDone, затем Close(ctx)]
//	                               +-- результат Close -> [CancelRun] -> результат
//	                               +-- ctx.Done -> [CancelRun] -> ctx.Err()
//	NATS -> [MessagesQueue] -> [Воркеры: Decoder -> Dispatcher -> Ack при успехе]
func NatsBotEventSubscriberProvider(
	lc fx.Lifecycle,
	GetJetStream BotEventJetStreamFactory,
	Decoder ports.BotEventDecoderPort,
	Dispatcher ports.BotEventDispatcherPort,
) ports.BotEventSubscriberPort {
	// Этап 1: очередь принадлежит подписчику; Close закроет её после остановки приёма.
	Subscriber := &bots_nats.NatsBotEventSubscriber{
		StreamName:          NATS_BOT_EVENT_STREAM,
		ConsumerName:        NATS_BOT_EVENT_CONSUMER,
		SubjectPrefix:       NATS_BOT_EVENT_SUBJECT_PREFIX,
		NatsBotEventDecoder: Decoder,
		BotEventDispatcher:  Dispatcher,
		MessagesQueue:       make(chan *nats.Msg, bots_nats.MESSAGES_QUEUE_LIMIT),
	}
	var CancelRun, CancelSubscribe context.CancelFunc // Устанавливаются в OnStart; разделяют отмену обработки и попыток подписки.
	var SubscribeDone chan struct{}                   // Закрывается при выходе цикла; исключает одновременные Start и Close в хуках.

	// Этап 2: запуск Fx не ждёт NATS; фоновые операции живут до остановки приложения.
	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			RunCtx, cancelRun := context.WithCancel(context.Background())
			SubscribeCtx, cancelSubscribe := context.WithCancel(RunCtx)
			CancelRun, CancelSubscribe = cancelRun, cancelSubscribe

			if err := Subscriber.CreateWorkers(RunCtx, BOT_EVENT_WORKERS_COUNT); err != nil {
				// При неуспешном OnStart Fx не вызовет этот OnStop: отменяем здесь.
				cancelSubscribe()
				cancelRun()
				return err
			}

			// Этап 3: один цикл владеет попытками Start; очередь и воркеры сохраняются.
			SubscribeDone = make(chan struct{})
			go func() {
				defer close(SubscribeDone)
				for SubscribeCtx.Err() == nil {
					JetStream, err := GetJetStream(SubscribeCtx)
					if err == nil && SubscribeCtx.Err() == nil {
						// Фабрика отвечает за доступность соединения, Start — за подписку
						// на существующий consumer; перед Start повторно проверяем отмену.
						Subscriber.Jetstream = JetStream
						err = Subscriber.Start(SubscribeCtx)
					}
					if err == nil || SubscribeCtx.Err() != nil {
						return
					}
					log.Printf("NATS: подписка на события ботов не создана: %v", err)

					Timer := time.NewTimer(BOT_EVENT_RETRY_DELAY)
					// Пауза ограничивает частоту ошибок и прерывается остановкой без ожидания 2 с.
					select {
					case <-SubscribeCtx.Done():
						Timer.Stop()
						return
					case <-Timer.C:
					}
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			// Этап 4: сначала запрещаем новые попытки; RunCtx оставляем обработке
			// очереди до завершения Close либо истечения stop-контекста.
			CancelSubscribe()
			defer CancelRun()

			// Даже при таймауте Close будет начат после завершения попытки Start.
			// Буфер позволяет отправить результат, если хук уже вернул ctx.Err().
			Stopped := make(chan error, 1)
			go func() {
				<-SubscribeDone
				Stopped <- Subscriber.Close(ctx)
			}()
			select {
			case err := <-Stopped:
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	return Subscriber
}
