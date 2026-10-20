package ports

import (
	"context"
	"reflect"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal/bots/events"
)

type BotEventRoutePort struct {
	Contract reflect.Type
	Event    events.BotEventI
}

type BotEventRoutePortI interface{}



type BotEventDecoderPort struct {
	routes []BotEventRoutePort
}

type BotEventDecoderPortI interface {
	Decode(
		ctx context.Context,
		routes []BotEventRoutePort,
	)
}

