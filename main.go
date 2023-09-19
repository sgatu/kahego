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
	"sgatu.com/kahego/src/stream"
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
	envConfig.BucketsFile = ""
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
	wgDataActors := &sync.WaitGroup{}
	dataActors := make(map[string]actors.Actor)
	for id, streamCfg := range bucketsConfig.Streams {
		strm, err := stream.GetStream(streamCfg)
		if err == nil {
			wgDataActors.Add(1)
			dataActor := actors.DataActor{Stream: strm, WaitGroup: wgDataActors}
			dataActors[id] = &dataActor
			actors.InitializeAndStart(&dataActor)
		} else {
			fmt.Println("Could not initialize dataActor due to", err)
		}
	}
	bucketActors := make(map[string]actors.Actor)
	for id, bucketCfg := range bucketsConfig.Buckets {
		bucketActor := actors.BucketActor{
			StreamActors: make([]actors.Actor, 0, len(bucketCfg.Streams)),
			Batch:        bucketCfg.Batch,
			BatchTimeout: bucketCfg.BatchTimeout,
		}
		for _, streamInfo := range bucketCfg.Streams {
			bucketActor.StreamActors = append(bucketActor.StreamActors, dataActors[streamInfo])
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
	<-done
	{
		for _, da := range dataActors {
			actors.Tell(da, actors.PoisonPill{})
		}
		listener.Stop()
	}
	wgListener.Wait()
	wgDataActors.Wait()
}
