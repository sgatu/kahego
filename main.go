package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"sgatu.com/kahego/src/actors"
	"sgatu.com/kahego/src/config"
)

func main() {

	errorChannel := make(chan struct{})
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
	if err == nil {
		fmt.Println("Loaded buckets and streams config:")
		cfgJson, err := json.MarshalIndent(bucketsConfig, "", " ")
		if err == nil {
			fmt.Println(string(cfgJson))
		}
	} else {
		fmt.Println("Could not load buckets and streams config due to err ->", err)
		os.Exit(1)
	}
	wgDataActorGateway := &sync.WaitGroup{}
	dataActorGateway := actors.DataActorGateway{
		WaitGroup:     wgDataActorGateway,
		StreamsConfig: bucketsConfig.Streams,
		ErrorChannel:  errorChannel,
	}
	wgDataActorGateway.Add(1)
	actors.InitializeAndStart(&dataActorGateway)

	bucketActors := make(map[string]actors.Actor)
	for id, bucketCfg := range bucketsConfig.Buckets {
		bucketActor := actors.BucketActor{
			StreamActors:     bucketCfg.Streams,
			Batch:            bucketCfg.Batch,
			BatchTimeout:     bucketCfg.BatchTimeout,
			DataGatewayActor: &dataActorGateway,
		}
		bucketActors[id] = &bucketActor
		actors.InitializeAndStart(&bucketActor)
	}
	wgListener := &sync.WaitGroup{}
	wgListener.Add(1)

	listener := actors.AcceptClientActor{SocketPath: envConfig.SocketPath, WaitGroup: wgListener, BucketActors: bucketActors}
	actors.InitializeAndStart(&listener)

	done := make(chan os.Signal, 1)

	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-errorChannel:
	case <-done:
		actors.Tell(&dataActorGateway, actors.PoisonPill{})
		listener.Stop()
		close(errorChannel)
		close(done)
	}
	fmt.Println("Waiting listener to close")
	wgListener.Wait()
	fmt.Println("Waiting gateway to close")
	wgDataActorGateway.Wait()
}
