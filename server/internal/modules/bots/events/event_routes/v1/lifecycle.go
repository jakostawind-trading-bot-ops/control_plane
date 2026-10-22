package eventroutesv1

import (
	contracts_bot_control_v1 "github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/contracts/bot/control/v1/common"
	lifecyclecontracts "github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/contracts/bot/control/v1/lifecycle"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/events"
	events_lifecycle_v1 "github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/events/v1/lifecycle"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/ports"
)

func BotStartedEventV1Route() ports.BotEventRoute {
	return ports.BotEventRoute{
		MessageType: "v1.lifecycle.bot_started_event",
		Contract: func() contracts_bot_control_v1.BaseBotEventContractV1I {
			return &lifecyclecontracts.BotStartedEventContractV1{}
		},
		BotEvent: func(contract contracts_bot_control_v1.BaseBotEventContractV1I) (events.BaseBotEventI, error) {
			Contract := contract.IsBaseBotEventContractV1()
			return &events_lifecycle_v1.BotStartedEventV1{
				BaseBotEvent: events.BaseBotEvent{
					BotID:     Contract.BotID,
					Payload:   Contract.Payload,
					Timestamp: Contract.Timestamp,
				},
			}, nil
		},
	}
}
