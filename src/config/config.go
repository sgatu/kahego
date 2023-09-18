package config

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	BucketsFile string
	SocketPath  string
}

type bucketsConfig struct {
	Streams []struct {
		ID       string            `json:"id"`
		Type     string            `json:"type"`
		Slice    float32           `json:"slice"`
		Settings map[string]string `json:"settings"`
	} `json:"streams"`
	Buckets []struct {
		Id      string `json:"id"`
		Streams []struct {
			Id  string
			Key string
		} `json:"streams"`
		Batch        int32 `json:"batch"`
		BatchTimeout int32 `json:"batchTimeout"`
	}
}
type StreamConfig struct {
	Type     string
	Slice    float32
	Settings map[string]string
}
type BucketConfig struct {
	Streams []struct {
		Id  string
		Key string
	}
	Batch        int32
	BatchTimeout int32
}
type MappedConfig struct {
	Streams map[string]StreamConfig
	Buckets map[string]BucketConfig
}

func getConfig(expected string, _default string) string {
	if expected == "" {
		return _default
	}
	return expected
}
func LoadConfig() (Config, error) {
	env := os.Getenv("environment")
	if env == "" {
		env = "dev"
	}
	err := godotenv.Load(".env." + env + ".local")
	if err != nil {
		return Config{}, err
	}
	bucketsFile := getConfig(os.Getenv("BUCKETS_FILE"), "./buckets.json")
	socketFile := getConfig(os.Getenv("SOCKET"), "/tmp/kahego.sock")
	return Config{
		BucketsFile: bucketsFile,
		SocketPath:  socketFile,
	}, nil
}
func LoadBucketsConfig(envConfig Config) (MappedConfig, error) {

	fmt.Println("Loading buckets file located at", envConfig.BucketsFile)
	var bucketsConfig bucketsConfig
	var mappedConfig MappedConfig
	configFile, err := os.Open(envConfig.BucketsFile)
	if err != nil {
		return mappedConfig, fmt.Errorf("could not read buckets config file at %s", envConfig.BucketsFile)
	}
	mappedConfig = MappedConfig{Streams: make(map[string]StreamConfig), Buckets: make(map[string]BucketConfig)}
	defer configFile.Close()
	jsonParser := json.NewDecoder(configFile)
	jsonParser.Decode(&bucketsConfig)
	for _, stream := range bucketsConfig.Streams {
		mappedConfig.Streams[stream.ID] = StreamConfig{Type: stream.Type, Slice: stream.Slice, Settings: stream.Settings}
	}
	for _, bucket := range bucketsConfig.Buckets {
		mappedConfig.Buckets[bucket.Id] = BucketConfig{Streams: bucket.Streams, Batch: bucket.Batch, BatchTimeout: bucket.BatchTimeout}
	}
	return mappedConfig, nil
}
