package di

import (
	"context"
	"errors"
	"log"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/fx"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
)

type NatsClient struct {
	BootstrapSettings internal.BootstrapSettings
	natsClient        atomic.Pointer[nats.Conn]
	cancel            context.CancelFunc
	done              chan struct{}
}

func NatsClientProvider(
	lc fx.Lifecycle,
	BootstrapSettings *internal.BootstrapSettings,
) *NatsClient {
	nats_client := &NatsClient{
		BootstrapSettings: *BootstrapSettings,
	}

	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			// The worker must outlive the startup hook's context.
			ctx, cancel := context.WithCancel(context.Background())
			nats_client.cancel = cancel
			nats_client.done = make(chan struct{})
			go nats_client.run(ctx)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if nats_client.cancel == nil {
				return nil
			}
			nats_client.cancel()
			select {
			case <-nats_client.done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})

	return nats_client
}

func (c *NatsClient) run(ctx context.Context) {
	defer close(c.done)
	const retryDelay = 2 * time.Second

	for ctx.Err() == nil {
		closed := make(chan struct{})
		conn, err := nats.Connect(
			c.BootstrapSettings.NatsUrl,
			nats.UserInfo(c.BootstrapSettings.NatsUser, c.BootstrapSettings.NatsPassword),
			nats.RetryOnFailedConnect(true),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(retryDelay),
			nats.ReconnectJitter(500*time.Millisecond, time.Second),
			nats.PingInterval(10*time.Second),
			nats.MaxPingsOutstanding(2),
			nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
				if ctx.Err() == nil && err != nil {
					log.Printf("NATS: соединение потеряно: %v", err)
				}
			}),
			nats.ReconnectHandler(func(_ *nats.Conn) {
				if ctx.Err() == nil {
					log.Println("NATS: соединение установлено")
				}
			}),
			nats.ClosedHandler(func(conn *nats.Conn) {
				if ctx.Err() == nil {
					log.Printf("NATS: соединение закрыто, будет создано заново: %v", conn.LastError())
				}
				close(closed)
			}),
		)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			var urlErr *url.Error
			if errors.As(err, &urlErr) {
				log.Println("NATS: некорректный URL подключения, проверьте настройки")
			} else {
				log.Printf("NATS: не удалось создать клиент, повторим попытку: %v", err)
			}
		} else {
			if ctx.Err() != nil {
				conn.Close()
				return
			}
			c.natsClient.Store(conn)
			log.Printf("NATS: клиент запущен, состояние: %s", conn.Status())
			select {
			case <-ctx.Done():
				conn.Close()
				c.natsClient.Store(nil)
				return
			case <-closed:
				c.natsClient.Store(nil)
			}
		}

		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
