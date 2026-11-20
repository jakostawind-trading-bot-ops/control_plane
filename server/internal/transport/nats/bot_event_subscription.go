package nats

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/nats-io/nats.go"
)

// Run запускает один фоновый цикл после CreateWorkers и сразу возвращает управление.
// ctx ограничивает время наблюдения. Close отменяет цикл и ждёт завершения попытки
// перед Drain. Повторный Run действующего экземпляра идемпотентен.
func (s *NatsBotEventSubscriber) Run(ctx context.Context) error {
	s.LifecycleMutex.Lock()
	defer s.LifecycleMutex.Unlock()
	if err := s.startError(ctx); err != nil {
		return err
	}
	if s.watchDone == nil {
		ctx, s.cancelWatch = context.WithCancel(ctx)
		s.watchDone = make(chan struct{})
		go func() {
			defer close(s.watchDone)
			s.watch(ctx)
		}()
	}
	return nil
}

// startError проверяет право начать попытку в ctx. Вызывается под LifecycleMutex.
func (s *NatsBotEventSubscriber) startError(ctx context.Context) error {
	if s.Closing {
		return IS_CLOSING
	}
	if !s.WorkersStarted {
		return WORKERS_NOT_STARTED
	}
	return ctx.Err()
}

// watch поддерживает одну подписку, пока активен ctx. Таймер нужен только после
// ошибки. Смена соединения прерывает задержку, закрытие подписки вызывает новый Bind.
//
//	снимок -> нет клиента -> ждать замены
//	       -> Bind -> ошибка -> ждать замены или таймера
//	               -> успех -> ждать замены или закрытия подписки
func (s *NatsBotEventSubscriber) watch(ctx context.Context) {
	const firstDelay, maxDelay = 2 * time.Second, 30 * time.Second
	delay := firstDelay
	for ctx.Err() == nil {
		sub, changed, err := s.ensureSubscription(ctx)
		if ctx.Err() != nil || errors.Is(err, IS_CLOSING) {
			return
		}
		var closed <-chan nats.SubStatus
		var retry <-chan time.Time
		switch {
		case err == nil:
			closed = sub.StatusChanged(nats.SubscriptionClosed)
			delay = firstDelay
		case errors.Is(err, NATS_NOT_CONNECTED), errors.Is(err, errConnectionChanged):
			// Ждём появления клиента; при устаревшем Bind канал уже закрыт.
		default:
			// Небольшой разброс распределяет запросы нескольких экземпляров.
			wait := delay - time.Duration(rand.Int64N(int64(delay/4)))
			s.Logger.WarnContext(ctx, "Не удалось подписаться на события ботов",
				"error", err, "retry_after", wait)
			retry = time.After(wait)
			delay = min(2*delay, maxDelay)
		}
		select {
		case <-ctx.Done():
			return
		case <-changed:
			delay = firstDelay
		case <-closed:
		case <-retry:
		}
	}
}

// ensureSubscription сериализует Run и прямые Start. Возвращает действующую
// подписку либо ошибку и канал замены снимка для следующего ожидания. ctx
// проверяется перед попыткой; сетевой Bind выполняется без LifecycleMutex.
//
//	снимок -> снять прежнюю привязку -> Bind -> проверить поколение
func (s *NatsBotEventSubscriber) ensureSubscription(ctx context.Context) (*nats.Subscription, <-chan struct{}, error) {
	s.subscribeMu.Lock()
	defer s.subscribeMu.Unlock()
	s.LifecycleMutex.Lock()
	err := s.startError(ctx)
	s.LifecycleMutex.Unlock()
	if err != nil {
		return nil, nil, err
	}
	if s.Snapshot == nil {
		return nil, nil, NATS_NOT_CONNECTED
	}
	js, changed, err := s.Snapshot()
	if s.Subscription != nil && s.boundTo != changed {
		if detachErr := s.detach(); detachErr != nil {
			return nil, changed, detachErr
		}
	}
	if err != nil {
		return nil, changed, err
	}
	if js == nil {
		return nil, changed, NATS_NOT_CONNECTED
	}
	if s.Subscription.IsValid() {
		return s.Subscription, changed, nil
	}

	sub, err := s.subscribe(ctx, js)
	if err != nil {
		return nil, changed, err
	}
	s.Subscription, s.boundTo = sub, changed
	select {
	case <-changed:
		// Даже успешный Bind мог завершиться уже после замены клиента.
		return nil, changed, errors.Join(errConnectionChanged, s.detach())
	default:
		s.Logger.InfoContext(ctx, "Подписчик событий ботов запущен", "subject", sub.Subject)
		return sub, changed, nil
	}
}

// errConnectionChanged сообщает прямому Start, что результат привязки устарел.
var errConnectionChanged = errors.New("соединение NATS заменено во время подписки")

// detach снимает привязку к старому соединению под subscribeMu. При ошибке
// сохраняет ещё действующую подписку для повтора. Bind сохраняет durable consumer.
func (s *NatsBotEventSubscriber) detach() error {
	if s.Subscription.IsValid() {
		if err := s.Subscription.Unsubscribe(); err != nil && s.Subscription.IsValid() {
			return err
		}
	}
	s.Subscription, s.boundTo = nil, nil
	return nil
}
