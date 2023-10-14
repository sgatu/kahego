package main

import (
	"encoding/json"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"sync"
	"syscall"

	"slices"

	"github.com/inhies/go-bytesize"
	log "github.com/sirupsen/logrus"
	easy "github.com/t-tomalak/logrus-easy-formatter"
	"sgatu.com/kahego/src/actors"
	"sgatu.com/kahego/src/config"
)

func initLogger(level string) {
	log.SetOutput(os.Stdout)
	formatter := &easy.Formatter{
		TimestampFormat: "2006-01-02 15:04:05",
		LogFormat:       "%lvl% | %time% | %msg%\n",
	}
	log.SetFormatter(formatter)
	allowedLevels := []string{"INFO", "WARN", "ERROR", "FATAL", "DEBUG", "TRACE"}
	if !slices.Contains(allowedLevels, level) {
		level = "INFO"
	}
	log.Infof("Setting log level to %s", level)
	switch level {

	case "WARN":
		log.SetLevel(log.WarnLevel)
	case "ERROR":
		log.SetLevel(log.ErrorLevel)
	case "FATAL":
		log.SetLevel(log.FatalLevel)
	case "DEBUG":
		log.SetLevel(log.DebugLevel)
	case "TRACE":
		log.SetLevel(log.TraceLevel)
	case "INFO":
	default:
		log.SetLevel(log.InfoLevel)
	}
}
func main() {

	envConfig, err := config.LoadConfig()
	if err != nil {
		log.Fatal("Environment file missing or could not be loaded.\nAn environment variable \"environment\" MUST be defined and file .env.{environment}.local must exist.\nBy default {environment} is dev.")
		os.Exit(1)
	}
	initLogger(os.Getenv("LOG_LEVEL"))
	log.Info("Setting max cpus usage to ", envConfig.MaxCpus)
	runtime.GOMAXPROCS(envConfig.MaxCpus)
	log.Info("Setting max memory usage to ", bytesize.New(float64(envConfig.MaxMemory)))
	debug.SetMemoryLimit(envConfig.MaxMemory)

	if _, err := os.Stat(envConfig.SocketPath); err == nil {
		log.Fatalf("Socket file at %s already exists. Check if not another process is already running, if so close it else try to delete it.", envConfig.SocketPath)
		os.Exit(1)
	}
	bucketsConfig, err := config.LoadBucketsConfigFromEnv(envConfig)
	if err == nil {
		log.Trace("Loaded buckets and streams config:")
		cfgJson, err := json.MarshalIndent(bucketsConfig, "", " ")
		if err == nil {
			log.Trace(string(cfgJson))
		}
	} else {
		log.Fatal("Could not load buckets and streams config due to err ->", err)
		os.Exit(1)
	}
	bucketsWaitGroup := &sync.WaitGroup{}
	bucketManagerActor := actors.BucketManagerActor{
		Actor:               &actors.BaseActor{},
		WaitableActor:       &actors.BaseWaitableActor{WaitGroup: bucketsWaitGroup},
		BucketsConfig:       bucketsConfig.Buckets,
		DefaultBucketConfig: bucketsConfig.DefaultBucket,
	}
	errBucket := actors.InitializeAndStart(&bucketManagerActor)
	if errBucket != nil {
		log.Fatalf("Could not start bucket manager, closing application. err: %s", errBucket)
	}

	listenerErrorChan := make(chan struct{})
	wgListener := &sync.WaitGroup{}
	listener := actors.AcceptClientActor{
		Actor: &actors.BaseActor{},
		WaitableActor: &actors.BaseWaitableActor{
			WaitGroup: wgListener,
		},
		SocketPath:        envConfig.SocketPath,
		BucketMangerActor: &bucketManagerActor,
		ListenerErrorChan: listenerErrorChan,
	}
	errListener := actors.InitializeAndStart(&listener)
	if errListener != nil {
		log.Fatalf("Could not start listener actor, closing application. err: %s", errListener)
	}
	closeSignalCh := make(chan os.Signal, 1)

	signal.Notify(closeSignalCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGPIPE)
	closeAll := func() {
		actors.Tell(&bucketManagerActor, actors.PoisonPill{})
		actors.Tell(&listener, actors.PoisonPill{})
	}

	select {
	case <-closeSignalCh:
		closeAll()
	case <-listenerErrorChan:
		closeAll()
	}
	log.Info("Waiting listener to close")
	wgListener.Wait()
	log.Info("Waiting buckets to close")
	bucketsWaitGroup.Wait()
}
