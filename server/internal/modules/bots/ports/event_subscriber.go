package ports

import "context"

type BotEventSubscriberPort interface {
	Start(ctx context.Context) error
	Close(ctx context.Context) error
}