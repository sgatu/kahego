package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"sgatu.com/kahego/src/actors"
	"sgatu.com/kahego/src/config"
)

func main() {
	envConfig, err := config.LoadConfig()
	if err != nil {
		fmt.Println("Environment file missing or could not be loaded.\nAn environment variable \"environment\" MUST be defined and file .env.{environment}.local must exist.\nBy default {environment} is dev.")
		os.Exit(1)
	}
	if _, err := os.Stat(envConfig.SocketPath); err == nil {
		fmt.Println("Socket file at", envConfig.SocketPath, "already exists. Check if not another process is already running, if so close it else try to delete it.")
		os.Exit(1)
	}
	bucketsConfig, err := config.LoadBucketsConfig(envConfig)
	if err != nil {
		fmt.Printf("%#v", bucketsConfig)

	}
	stoppedAndCleanedUp := make(chan struct{})
	listener := actors.AcceptClientActor{SocketPath: envConfig.SocketPath, StopAndCleanedUp: stoppedAndCleanedUp}
	actors.InitializeAndStart(&listener)

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-done
		actors.Tell(&listener, actors.PoisonPill{})
	}()
	<-stoppedAndCleanedUp
	os.Exit(0)
}
