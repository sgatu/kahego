package main

import (
	"fmt"
	"os"

	"sgatu.com/kahego/src/config"
	//"sgatu.com/kahego/src/stream"
	//"github.com/spf13/viper"
)

func main() {
	envConfig, err := config.LoadConfig()
	if err != nil {
		fmt.Println("Environment file missing or could not be loaded.\nAn environment variable \"environment\" MUST be defined and file .env.{environment}.local must exist.\nBy default {environment} is dev.")
		os.Exit(1)
	}
	fmt.Printf("%v", config.LoadBucketsConfig(envConfig))
	//stream.GetStream(nil)

	// fmt.Println("Will connect to", fmt.Sprintf("%s:%s", os.Getenv("KAFKA_HOST"), os.Getenv("KAFKA_PORT")))
}
