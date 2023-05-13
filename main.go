package main

import (
	_ "MxcMQ/config"
	"MxcMQ/logger"
	_ "MxcMQ/persist"
	_ "MxcMQ/registrationCenter"
	"MxcMQ/server"
)

func main() {
	server := server.NewServerFromConfig()
	if err := server.Online(); err != nil {
		logger.Errorf("Online failed: %v", err)
	}

	server.RunWithGrpc()
}
