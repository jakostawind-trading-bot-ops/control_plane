package decoder_routes_v1

import (
	"encoding/json/v2"
	"fmt"

	lifecyclecontracts "github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/contracts/bot/control/v1/lifecycle"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/events"
	events_lifecycle "github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/events/events/lifecycle"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/modules/bots/ports"
)

// BotStartedEventV1DecoderRoute возвращает маршрут сообщения о запуске бота
// в формате V1. Маршрут создаётся для реестра декодера и возвращает внутренний
// *BotStartedEvent, общий для транспортных версий с тем же смыслом события.
//
// Результат — маршрут с ключом v1.lifecycle.bot_started_event и функцией Decode.
// Decode получает RawMessage с JSON, создаёт новый контракт на каждый вызов
// и возвращает событие либо ошибку разбора JSON или валидации контракта V1.
//
// Этапы Decode:
//  1. Десериализовать RawMessage в отдельный BotStartedEventContractV1.
//  2. Проверить контракт V1 до создания внутреннего события.
//  3. Перенести поля контракта в BotStartedEvent.
//
// Схема:
//
//	RawMessage -> [JSON -> Контракт V1] -> [Validate] -> BotStartedEvent
//	                       |                  |
//	                     error              error
func BotStartedEventV1DecoderRoute() ports.BotEventDecoderRoute {
	return ports.BotEventDecoderRoute{
		MessageType: "v1.lifecycle.bot_started_event",
		Decode: func(RawMessage []byte) (events.BaseBotEventI, error) {
			// Этап 1: отдельный контракт исключает перенос данных между сообщениями.
			Contract := &lifecyclecontracts.BotStartedEventContractV1{}
			if err := json.Unmarshal(RawMessage, Contract); err != nil {
				return nil, fmt.Errorf("разбор JSON: %w", err)
			}

			// Этап 2: проверяем обязательные поля до передачи данных приложению.
			if err := Contract.Validate(); err != nil {
				return nil, fmt.Errorf("валидация контракта: %w", err)
			}

			// Этап 3: приложение получает событие без привязки к транспортной версии.
			return &events_lifecycle.BotStartedEvent{
				BaseBotEvent: events.BaseBotEvent{
					BotID:     Contract.BotID,
					Payload:   Contract.Payload,
					Timestamp: Contract.Timestamp,
				},
			}, nil
		},
	}
}
