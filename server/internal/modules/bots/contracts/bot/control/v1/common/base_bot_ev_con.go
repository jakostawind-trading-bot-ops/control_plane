package contracts_bot_control_v1

import (
	"fmt"
	"strings"
)

type BaseBotEventContractV1 struct {
	BotID     string      `json:"bot_id"`
	Payload   map[any]any `json:"payload"`
	Timestamp string      `json:"timestamp"`
}

type BaseBotEventContractV1I interface {
	IsBaseBotEventContractV1() *BaseBotEventContractV1
	Validate() error
}

func (BaseBotEventContractV1 *BaseBotEventContractV1) IsBaseBotEventContractV1() *BaseBotEventContractV1 {
	return BaseBotEventContractV1
}

func (BaseBotEventContractV1 *BaseBotEventContractV1) Validate() error {
	// контракт отсутсвует
	if BaseBotEventContractV1 == nil {
		return fmt.Errorf("Контракт отсутсвует")
	}
	// BotID отсуствует
	if strings.TrimSpace(BaseBotEventContractV1.BotID) == "" {
		return fmt.Errorf("BotID обязателен")
	}
	// Timestamp отсутсвует
	if strings.TrimSpace(BaseBotEventContractV1.Timestamp) == "" {
		return fmt.Errorf("Timestamp обязателен")
	}

	return nil
}
