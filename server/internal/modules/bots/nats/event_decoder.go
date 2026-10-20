package nats

import (
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/bots/events"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/bots/ports"
)

type NatsBotEventRouteI interface {
	ports.BotEventRoutePortI
	GenerateSuffix(msg events.BotEvent)
}

func (d *NatsBotEventRouteI) GenerateSuffix(
	
)