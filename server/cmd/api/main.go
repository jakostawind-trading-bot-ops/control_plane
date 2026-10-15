package main

import (
	"fmt"

	"github.com/jakostawind-trading-bot-ops/control_plane/internal"
)

func main() {
	bootstrap_settings := internal.LoadBootstrapSettings()

	fmt.Println(bootstrap_settings)
}