package internal

import "os"

type BootstrapSettings struct {
	Stage string

	NatsUrl      string
	NatsUser     string
	NatsPassword string
}

func LoadBootstrapSettings() BootstrapSettings {
	return BootstrapSettings{
		Stage: os.Getenv("STAGE"),

		NatsUrl:      os.Getenv("NATS_URL"),
		NatsUser:     os.Getenv("NATS_USER"),
		NatsPassword: os.Getenv("NATS_PASSWORD"),
	}
}
