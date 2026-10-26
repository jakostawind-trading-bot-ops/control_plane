package nats

import (
	"context"
	"testing"
)

// TestGetMessageType проверяет выделение типа при wildcard и буквальном префиксе.
// t используется для сообщения об ошибках; проверка не требует NATS-сервера.
func TestGetMessageType(t *testing.T) {
	Cases := []struct {
		Name      string // Название проверяемого сценария.
		Prefix    string // Префикс подписки, допускающий отдельный токен *.
		Subject   string // Конкретный subject доставленного сообщения.
		Want      string // Ожидаемый ключ маршрута декодера.
		WantError bool   // Ожидается отказ при несовпадении префикса или отсутствии типа.
	}{
		{Name: "bot_id", Prefix: "bot.*.to.control.event.", Subject: "bot.bot-1.to.control.event.v1.lifecycle.bot_started_event", Want: "v1.lifecycle.bot_started_event"},
		{Name: "different_bot", Prefix: "bot.*.to.control.event.", Subject: "bot.bot-2.to.control.event.v1.lifecycle.bot_started_event", Want: "v1.lifecycle.bot_started_event"},
		{Name: "literal_prefix", Prefix: "events.", Subject: "events.v1.lifecycle.bot_started_event", Want: "v1.lifecycle.bot_started_event"},
		{Name: "empty_bot_id", Prefix: "bot.*.to.control.event.", Subject: "bot..to.control.event.v1.lifecycle.bot_started_event", WantError: true},
		{Name: "multiple_bot_tokens", Prefix: "bot.*.to.control.event.", Subject: "bot.one.two.to.control.event.v1.lifecycle.bot_started_event", WantError: true},
		{Name: "wrong_root", Prefix: "bot.*.to.control.event.", Subject: "other.bot-1.to.control.event.v1.lifecycle.bot_started_event", WantError: true},
		{Name: "wrong_direction", Prefix: "bot.*.to.control.event.", Subject: "bot.bot-1.from.control.event.v1.lifecycle.bot_started_event", WantError: true},
		{Name: "missing_type", Prefix: "bot.*.to.control.event.", Subject: "bot.bot-1.to.control.event", WantError: true},
		{Name: "empty_type", Prefix: "bot.*.to.control.event.", Subject: "bot.bot-1.to.control.event.", WantError: true},
		{Name: "wildcard_is_not_bot_id", Prefix: "bot.*.to.control.event.", Subject: "bot.*.to.control.event.v1.lifecycle.bot_started_event", WantError: true},
	}
	for _, Case := range Cases {
		t.Run(Case.Name, func(t *testing.T) {
			Subscriber := &NatsBotEventSubscriber{SubjectPrefix: Case.Prefix}
			Got, err := Subscriber.GetMessageType(context.Background(), Case.Subject)
			if (err != nil) != Case.WantError {
				t.Fatalf("error = %v, want error %v", err, Case.WantError)
			}
			if Got != Case.Want {
				t.Errorf("type = %q, want %q", Got, Case.Want)
			}
		})
	}
}
