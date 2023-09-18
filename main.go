package main

import (
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
	bucketsConfig, err := config.LoadBucketsConfig(envConfig)
	if err != nil {
		fmt.Printf("%#v", bucketsConfig)
	}
	/*
		//queue testing
		queue := datastructures.NewQueue[stream.Message]()
		for i := 0; i < 50; i++ {
			queue.Push(&datastructures.Node[stream.Message]{
				Value: stream.Message{
					Data:   make([]byte, 0),
					Bucket: "mybucket",
					Key:    fmt.Sprintf("msg_key_%d", i),
				},
			})
		}
		fmt.Println(queue.Len())
		len := queue.Len()
		for i := 1; i < int(len)+1; i++ {
			elem, err := queue.Pop()
			if err != nil {
				fmt.Println("loop", err)
				break
			}
			fmt.Printf("%v\n", elem.Value.Key)
			if i%5 == 0 {
				queue.Push(&datastructures.Node[stream.Message]{
					Value: stream.Message{
						Data:   make([]byte, 0),
						Bucket: "mybucket",
						Key:    fmt.Sprintf("msg_key_%d", i),
					},
				})
				fmt.Printf("Len: %d\n", queue.Len())
			}
		}
		elem, err := queue.Pop()
		if err != nil {
			fmt.Println("out of loop", err)
		} else {
			fmt.Printf("%+v --- %d\n", elem.Value.Key, queue.Len())
		}
		os.Exit(0)*/
	dataActors := make(map[string]actors.DataActor)
	for id, streamCfg := range bucketsConfig.Streams {
		strm, err := stream.GetStream(streamCfg)
		if err != nil {
			dataActor := actors.DataActor{Stream: strm}
			dataActors[id] = dataActor
		}
	}
	for id, bucketCfg := range bucketsConfig.Buckets {
		bucket := stream.Bucket{Streams: make(map[string]stream.Stream), Id: id, Batch: bucketCfg.Batch}
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	listener := actors.AcceptClientActor{SocketPath: envConfig.SocketPath, WaitGroup: wg}
	actors.InitializeAndStart(&listener)

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	<-done
	{
		listener.Stop()
	}
	wg.Wait()
}
