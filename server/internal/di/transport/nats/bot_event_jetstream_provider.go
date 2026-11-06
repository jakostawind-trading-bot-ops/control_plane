package di

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
)

// BotEventJetStreamFactory получает JetStream после подключения NATS.
// ctx отменяет ожидание соединения. Результат — JetStreamContext либо ошибка
// ожидания/создания. Запросы JetStream ограничены таймаутом в три секунды.
type BotEventJetStreamFactory func(ctx context.Context) (nats.JetStreamContext, error)

// NatsBotEventJetStreamProvider возвращает функцию отложенного получения
// JetStream. NatsClient — владелец соединения, создаваемый общим провайдером.
// Конструктор не ждёт подключения; возвращённую функцию вызывают после OnStart.
//
// Схема:
//
//	Вызов фабрики -> [WaitConnection(ctx)] -> [Conn.JetStream] -> результат
func NatsBotEventJetStreamProvider(
	NatsClient *NatsClient,
) BotEventJetStreamFactory {
	return func(ctx context.Context) (nats.JetStreamContext, error) {
		Conn, err := NatsClient.WaitConnection(ctx)
		if err != nil {
			return nil, err
		}
		// Ограничиваем Bind-запрос, сохраняя явное закрытие через Drain.
		return Conn.JetStream(nats.MaxWait(3 * time.Second))
	}
}
