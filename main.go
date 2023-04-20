package main

import (
	"MxcMQ/server"
)

func main() {
	si := server.ServerInfo{}
	server := server.NewServer(si)

	server.Run()
}
