package nats

import "github.com/nats-io/nats.go"

type NatsBotEventSubscriber struct {
	Jetstream nats.JetStreamContext
	StreamName string
	SubjectPrefix string
	ConsumerName string
}