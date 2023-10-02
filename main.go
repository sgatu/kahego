package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"sync"
	"syscall"

	"github.com/inhies/go-bytesize"
	"sgatu.com/kahego/src/actors"
	"sgatu.com/kahego/src/config"
)

func main() {
	envConfig, err := config.LoadConfig()
	if err != nil {
		fmt.Println("Environment file missing or could not be loaded.\nAn environment variable \"environment\" MUST be defined and file .env.{environment}.local must exist.\nBy default {environment} is dev.")
		os.Exit(1)
	}
	fmt.Println("Setting max cpus usage to", envConfig.MaxCpus)
	runtime.GOMAXPROCS(envConfig.MaxCpus)
	fmt.Println("Setting max memory usage to", bytesize.New(float64(envConfig.MaxMemory)))
	debug.SetMemoryLimit(envConfig.MaxMemory)
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
	bucketsWaitGroup := &sync.WaitGroup{}
	bucketManagerActor := actors.BucketManagerActor{
		Actor:               &actors.BaseActor{},
		WaitableActor:       &actors.BaseWaitableActor{WaitGroup: bucketsWaitGroup},
		BucketsConfig:       bucketsConfig.Buckets,
		DefaultBucketConfig: bucketsConfig.DefaultBucket,
	}
	actors.InitializeAndStart(&bucketManagerActor)

	wgListener := &sync.WaitGroup{}
	listener := actors.AcceptClientActor{
		Actor: &actors.BaseActor{},
		WaitableActor: &actors.BaseWaitableActor{
			WaitGroup: wgListener,
		},
		OrderedMessagesActor: &actors.BaseOrderedMessagesActor{},
		SocketPath:           envConfig.SocketPath,
		BucketMangerActor:    &bucketManagerActor,
	}
	actors.InitializeAndStart(&listener)
	closeSignalCh := make(chan os.Signal, 1)

	signal.Notify(closeSignalCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGPIPE)
	closeAll := func() {
		actors.Tell(&bucketManagerActor, actors.PoisonPill{})
		actors.Tell(&listener, actors.PoisonPill{})
	}

	<-closeSignalCh
	closeAll()

	fmt.Fprintln(os.Stderr, "Waiting listener to close")
	wgListener.Wait()
	fmt.Fprintln(os.Stderr, "Waiting buckets to close")
	bucketsWaitGroup.Wait()
}
