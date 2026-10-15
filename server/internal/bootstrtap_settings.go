package internal

import "os"

type BootstrapSettings struct {
	stage string

	nats_url string
	nats_user string
	nats_password string
}

func LoadBootstrapSettings() BootstrapSettings {
	return BootstrapSettings{
		stage: os.Getenv("STAGE"),

		nats_url: os.Getenv("NATS_URL"),
		nats_user: os.Getenv("NATS_USER"),
		nats_password: os.Getenv("NATS_PASSWORD"),
	}
}