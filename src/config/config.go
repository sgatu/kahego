package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type Config struct {
	bucketsFile string
	socketPath  string
}
type StreamConfig struct {
	id       string            `mapstructure:"id"`
	t        string            `mapstructure:"t"`
	slice    float32           `mapstructure:"slice"`
	settings map[string]string `mapstructure:"settings"`
}
type BucketConfig struct {
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
		bucketsFile: bucketsFile,
		socketPath:  socketFile,
	}, nil
}
func LoadBucketsConfig(config Config) []StreamConfig {
	fmt.Println("Loading", config.bucketsFile)
	v := viper.New()
	v.SetConfigFile(config.bucketsFile)
	v.SetConfigType("json")
	fmt.Printf("%v", v.ReadInConfig())
	var streamsInfo []StreamConfig
	fmt.Printf("Unmarshal...%+v\n", v.UnmarshalKey("streams", &streamsInfo))
	return streamsInfo
}
