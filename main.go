package main

import (
	"MxcMQ/server"
)

func main() {
	server := server.NewServerFromConfig()
	server.RunWithGrpc()
}
