package nats

import (
	"log"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
	"github.com/nats-io/nats.go"
)

type NatsClient struct {
	BootstrapSettings internal.BootstrapSettings
	natsClient        *nats.Conn
}

func CreateConnection(NatsClient *NatsClient) error {
	nc, err := nats.Connect(
		NatsClient.BootstrapSettings.NatsUrl,
		nats.UserInfo(
			NatsClient.BootstrapSettings.NatsUser,
			NatsClient.BootstrapSettings.NatsPassword,
		),
	)

	if err != nil {
		return err
	}

	NatsClient.natsClient = nc
	log.Println("Успешное подключение к NATS")

	return nil
}

func CloseConnection(NatsClient *NatsClient) {
	log.Println("Попытка закрыть соединения с NATS")
	NatsClient.natsClient.Close()
	log.Println("Соединение NATS закрыто")
}
