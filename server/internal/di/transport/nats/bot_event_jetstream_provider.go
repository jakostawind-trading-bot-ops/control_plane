package di_transport_nats

import (
	"time"

	"github.com/nats-io/nats.go"
)

// BotEventJetStreamFactory возвращает JetStream текущего соединения и сигнал
// его замены. Пока соединения нет, возвращает nil и открытый канал.
type BotEventJetStreamFactory func() (nats.JetStreamContext, <-chan struct{}, error)

// NatsBotEventJetStreamProvider связывает снимок клиента с JetStream для Bind.
// client владеет соединением. MaxWait ограничивает запрос к брокеру, сохраняя
// время жизни успешной подписки до явного Drain.
func NatsBotEventJetStreamProvider(Сlient *NatsClient) BotEventJetStreamFactory {
	return func() (nats.JetStreamContext, <-chan struct{}, error) {
		Сonn, Сhanged := Сlient.connectionSnapshot()
		if Сonn == nil {
			return nil, Сhanged, nil
		}
		js, err := Сonn.JetStream(nats.MaxWait(3 * time.Second))
		return js, Сhanged, err
	}
}
