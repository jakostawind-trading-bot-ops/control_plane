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
	MESSAGES_QUEUE_LIMIT = 100
)

// Errors
var (
	NATS_NOT_CONNECTED        = errors.New("NatsBotEventSubscriber: нет подключения к NATS")
	NATS_NOT_SUBSCRIBED       = errors.New("NatsBotEventSubscriber: нет подписки на subject")
	CANNOT_SUBSCRIBE          = errors.New("NatsBotEventSubscriber: не смог подписаться")
	IS_CLOSING                = errors.New("NatsBotEventSubscriber: остановка уже началась")
	WORKERS_ALREADY_STARTED   = errors.New("NatsBotEventSubscriber: workers уже запущены")
	WORKERS_NOT_STARTED       = errors.New("NatsBotEventSubscriber: воркеры не запущены. Сначала CreateWorkers")
	INVALID_WORKER_COUNT      = errors.New("NatsBotEventSubscriber: неверное количество воркеров")
	MESSAGE_QUEUE_NOT_CREATED = errors.New("NatsBotEventSubscriber: MessageQueue не создана. Нужно создать в di")
	CLOSE_SUBSCRIPTION 		= errors.New("NatsBotEventSubscriber: остановка подписки NATS")
)

type NatsBotEventSubscriber struct {
	Subscription        *nats.Subscription
	Jetstream           nats.JetStreamContext
	StreamName          string
	SubjectPrefix       string
	ConsumerName        string
	NatsBotEventDecoder ports.BotEventDecoderPort
	BotEventDispatcher  ports.BotEventDispatcherPort
	LifecycleMutex      sync.Mutex
	WorkersWG           sync.WaitGroup
	WorkersStarted      bool
	MessageQueueMutex   sync.RWMutex
	MessagesQueue       chan *nats.Msg
	MessageQueueClosed  bool
	Closing             bool
	CloseDone           chan struct{}
	CloseErr            error
}

func (NatsBotEventSubscriber *NatsBotEventSubscriber) Start(ctx context.Context) error {
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

	Subject := NatsBotEventSubscriber.SubjectPrefix + ">"

	MessageQueue := NatsBotEventSubscriber.MessagesQueue
	Subscription, err := NatsBotEventSubscriber.Jetstream.Subscribe(
		Subject,
		func(Message *nats.Msg) {
			NatsBotEventSubscriber.MessageQueueMutex.RLock()
			defer NatsBotEventSubscriber.MessageQueueMutex.RUnlock()
			if NatsBotEventSubscriber.MessageQueueClosed {
				return
			}
			MessageQueue <- Message
		},
		// nats.Durable(NatsBotEventSubscriber.ConsumerName),
		// nats.BindStream(NatsBotEventSubscriber.StreamName),
		nats.Bind(NatsBotEventSubscriber.StreamName, NatsBotEventSubscriber.ConsumerName),
		nats.ManualAck(),
	)

	if err != nil {
		return errors.Join(CANNOT_SUBSCRIBE, err)
	}

	NatsBotEventSubscriber.Subscription = Subscription

	return nil
}

func (NatsBotEventSubscriber *NatsBotEventSubscriber) HandleMessage(
	ctx context.Context,
	Message *nats.Msg,
) {
	MessageType, err := NatsBotEventSubscriber.GetMessageType(
		ctx, Message.Subject,
	)
	if err != nil {
		fmt.Printf("ошибка определения типа сообщения: %v\n", err)
		return
	}

	BotEvent, err := NatsBotEventSubscriber.NatsBotEventDecoder.BotEventDecode(
		ctx, MessageType, Message.Data,
	)
	if err != nil {
		fmt.Printf("ошибка декодирования: %v\n", err)
		return
	}

	err = NatsBotEventSubscriber.BotEventDispatcher.BotEventDispatch(
		ctx, BotEvent,
	)
	if err != nil {
		fmt.Printf("ошибка обработки события: %v\n", err)
		return
	}

	if err := Message.Ack(); err != nil {
		fmt.Printf("ошибка подтверждения сообщения: %v\n", err)
	}

}

func (NatsBotEventSubscriber *NatsBotEventSubscriber) GetMessageType(
	ctx context.Context,
	Subject string,
) (string, error) {
	MessageType, found := strings.CutPrefix(Subject, NatsBotEventSubscriber.SubjectPrefix)
	if !found {
		return "", fmt.Errorf("Неподходящий префикс: %s", Subject)
	}

	return MessageType, nil
}

func (NatsBotEventSubscriber *NatsBotEventSubscriber) CreateWorkers(
	ctx context.Context,
	Count int,
) error {
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

	MessageQueue := NatsBotEventSubscriber.MessagesQueue
	NatsBotEventSubscriber.WorkersWG.Add(Count)
	NatsBotEventSubscriber.WorkersStarted = true

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

func (NatsBotEventSubscriber *NatsBotEventSubscriber) Close(
	ctx context.Context,
) error {
	NatsBotEventSubscriber.LifecycleMutex.Lock()

	if !NatsBotEventSubscriber.Closing {
		NatsBotEventSubscriber.Closing = true
		NatsBotEventSubscriber.CloseDone = make(chan struct{})
		Subscription := NatsBotEventSubscriber.Subscription
		MessagesQueue := NatsBotEventSubscriber.MessagesQueue
		
		go func() {
			defer close(NatsBotEventSubscriber.CloseDone)

			if Subscription != nil {
				Closed := NatsBotEventSubscriber.Subscription.StatusChanged(nats.SubscriptionClosed)
				if err := Subscription.Drain(); err != nil {
					NatsBotEventSubscriber.CloseErr = errors.Join(CLOSE_SUBSCRIPTION, err)
				} else {
					<- Closed
				}
			}

			NatsBotEventSubscriber.MessageQueueMutex.Lock()
			NatsBotEventSubscriber.MessageQueueClosed = true

			if MessagesQueue != nil {
				close(MessagesQueue)
			}
			NatsBotEventSubscriber.MessageQueueMutex.Unlock()
			NatsBotEventSubscriber.WorkersWG.Wait()
		}()
	}

	CloseDone := NatsBotEventSubscriber.CloseDone
	NatsBotEventSubscriber.LifecycleMutex.Unlock()

	select {
	case <- CloseDone:
		return NatsBotEventSubscriber.CloseErr 
	case <- ctx.Done():
		return ctx.Err()
	}
}
