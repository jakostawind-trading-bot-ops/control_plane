package shared

import (
	"context"
	"log"

	"github.com/nats-io/nats.go"
	"go.uber.org/fx"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
)

type NatsClient struct {
	BootstrapSettings internal.BootstrapSettings
	natsClient        *nats.Conn
}

func NatsClientProvider(
	lc fx.Lifecycle,
	BootstrapSettings *internal.BootstrapSettings,
) *NatsClient {
	nats_client := &NatsClient{
		BootstrapSettings: *BootstrapSettings,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			conn, err := nats.Connect(
				nats_client.BootstrapSettings.NatsUrl,
				nats.UserInfo(
					nats_client.BootstrapSettings.NatsUser,
					nats_client.BootstrapSettings.NatsPassword,
				),
			)
			if err != nil {
				return err
			}

			nats_client.natsClient = conn
			log.Println("Успешное подключение к NATS")

			return nil
		},
		OnStop: func(ctx context.Context) error {
			nats_client.natsClient.Close()
			return nil
		},
	})

	return nats_client
}