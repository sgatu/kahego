package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/vladopajic/go-actor/actor"
	"sgatu.com/kahego/src/config"
	//"sgatu.com/kahego/src/stream"
)

type SocketActor struct {
	socketPath    string
	socket        net.Listener
	acceptChannel chan net.Conn
	mainChannel   chan string
}

func (sworker *SocketActor) onStart() {
	socket, err := net.Listen("unix", sworker.socketPath)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Socket created...")
	sworker.socket = socket
	exitAppSignal := make(chan os.Signal, 1)
	signal.Notify(exitAppSignal, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-exitAppSignal
		sworker.cleanUp()
	}()
	sworker.acceptChannel = make(chan net.Conn)
	go func(l net.Listener) {
		for {
			conn, err := l.Accept()
			if err != nil {
				fmt.Println("Error while accepting new socket connections")
				sworker.acceptChannel <- nil
				return
			}
			fmt.Println("Client connected")
			sworker.acceptChannel <- conn
		}
	}(sworker.socket)

}
func (sworker *SocketActor) onStop() {
	sworker.cleanUp()
}
func (sworker *SocketActor) cleanUp() {
	fmt.Println("Doing cleanup...")
	if sworker.socket != nil {
		sworker.socket.Close()
		os.Remove(sworker.socketPath)
	}
}

func handleConnection(conn net.Conn) {
	for {
		data := make([]byte, 500)
		len, err := conn.Read(data)
		if err != nil {
			return
		}
		fmt.Println("len of", fmt.Sprintf("%d", len), "data: ", string(data))
	}
}
func (sworker *SocketActor) DoWork(c actor.Context) actor.WorkerStatus {
	select {
	case conn := <-sworker.acceptChannel:
		if conn == nil {
			sworker.cleanUp()
			sworker.mainChannel <- "socket_closed"
			return actor.WorkerEnd
		} else {
			go handleConnection(conn)
			return actor.WorkerContinue
		}
	case <-c.Done():
		return actor.WorkerEnd
	}
}
func main() {
	envConfig, err := config.LoadConfig()
	if err != nil {
		fmt.Println("Environment file missing or could not be loaded.\nAn environment variable \"environment\" MUST be defined and file .env.{environment}.local must exist.\nBy default {environment} is dev.")
		os.Exit(1)
	}
	bucketsConfig, err := config.LoadBucketsConfig(envConfig)
	if err != nil {
		fmt.Printf("%#v", bucketsConfig)

	}
	channelMain := make(chan string)
	socketWorker := &SocketActor{socketPath: envConfig.SocketPath, mainChannel: channelMain}
	socketActor := actor.New(
		socketWorker,
		actor.OptOnStart(socketWorker.onStart),
		actor.OptOnStop(socketWorker.onStop),
	)
	socketActor.Start()
	code := <-channelMain
	switch code {
	case "socket_closed":
		fmt.Println("Socket closed, exiting service")
		os.Exit(0)
	}

	//stream.GetStream(nil)

	// fmt.Println("Will connect to", fmt.Sprintf("%s:%s", os.Getenv("KAFKA_HOST"), os.Getenv("KAFKA_PORT")))
}
