package ports

import (
	"context"

	contracts_bot_control_v1 "github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/contracts/bot/control/v1/common"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/events"
)

type BotEventRoute struct {
	MessageType string
	Contract    func() contracts_bot_control_v1.BaseBotEventContractV1I
	BotEvent    func(
		contract contracts_bot_control_v1.BaseBotEventContractV1I,
	) (events.BaseBotEventI, error)
}

type BotEventDecoderPort interface {
	BotEventDecode(ctx context.Context, MessageType string, RawMessage []byte) (events.BaseBotEventI, error)
}
