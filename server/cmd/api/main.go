package main

import (
	"fmt"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
	"github.com/jakostawind-trading-bot-ops/control_plane/internal/shared/nats"
)

func main() {
	BootstrapSettings := internal.LoadBootstrapSettings()

	// NATS
	NatsClient := &nats.NatsClient{
		BootstrapSettings: BootstrapSettings,
	}

	if err := nats.CreateConnection(NatsClient); err != nil {
		fmt.Println(err)
	}

	defer nats.CloseConnection(NatsClient)
}
