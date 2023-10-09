package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/inhies/go-bytesize"
	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
)

type EnvConfig struct {
	BucketsFile string
	SocketPath  string
	MaxCpus     int
	MaxMemory   int64
}
type jsonBucketConfig struct {
	Id           string   `json:"id"`
	Streams      []string `json:"streams"`
	Batch        int32    `json:"batch"`
	BatchTimeout int32    `json:"batchTimeout"`
}
type bucketsConfig struct {
	Streams map[string]struct {
		Type     string            `json:"type"`
		Slice    float32           `json:"slice"`
		Settings map[string]string `json:"settings"`
		Backup   *struct {
			Type     string            `json:"type"`
			Settings map[string]string `json:"settings"`
		} `json:"backup"`
	} `json:"streamsConfigs"`
	Buckets       []jsonBucketConfig
	DefaultBucket *jsonBucketConfig `json:"defaultBucket"`
}
type StreamConfig struct {
	Type     string
	Slice    float32
	Settings map[string]string
	Backup   *BackupStreamConfig
}
type BackupStreamConfig struct {
	Type     string
	Settings map[string]string
}

type BucketConfig struct {
	BucketId      string
	StreamConfigs map[string]StreamConfig
	Batch         int32
	BatchTimeout  int64
}
type MappedConfig struct {
	Buckets       map[string]*BucketConfig
	DefaultBucket *BucketConfig
}

func getConfig(expected string, _default string) string {
	if expected == "" {
		return _default
	}
	return expected
}

func LoadConfig() (EnvConfig, error) {
	env := os.Getenv("environment")
	if env == "" {
		env = "dev"
	}
	err := godotenv.Load(".env." + env + ".local")
	if err != nil {
		return EnvConfig{}, err
	}
	bucketsFile := getConfig(os.Getenv("BUCKETS_FILE"), "./buckets.json")
	socketFile := getConfig(os.Getenv("SOCKET"), "/tmp/kahego.sock")
	maxCpus, err := strconv.ParseInt(getConfig(os.Getenv("MAXCPUS"), "2"), 10, 64)
	if err != nil {
		maxCpus = 2
	}
	maxMemory, err := bytesize.Parse(getConfig(os.Getenv("MAXMEMORY"), "250MB"))
	if err != nil {
		mm, _ := bytesize.Parse("250MB")
		maxMemory = mm
	}

	return EnvConfig{
		BucketsFile: bucketsFile,
		SocketPath:  socketFile,
		MaxCpus:     int(maxCpus),
		MaxMemory:   int64(maxMemory),
	}, nil
}
func LoadBucketsConfigFromString(ConfigStr string) (*MappedConfig, error) {
	var bucketsConfig bucketsConfig

	configFile := strings.NewReader(ConfigStr)
	jsonParser := json.NewDecoder(configFile)
	jsonParser.Decode(&bucketsConfig)
	if len(bucketsConfig.Streams) == 0 {
		return nil, fmt.Errorf("no streams defined")
	}
	if len(bucketsConfig.Buckets) == 0 {
		return nil, fmt.Errorf("no buckets defined")
	}
	streamsConfig := make(map[string]StreamConfig)
	for k, stream := range bucketsConfig.Streams {
		streamConfig := StreamConfig{Type: stream.Type, Slice: stream.Slice, Settings: stream.Settings}
		if stream.Backup != nil {
			streamConfig.Backup = &BackupStreamConfig{
				Type:     stream.Backup.Type,
				Settings: stream.Backup.Settings,
			}
		}
		streamsConfig[k] = streamConfig
	}
	mappedConfig := MappedConfig{Buckets: make(map[string]*BucketConfig), DefaultBucket: nil}
	for _, bucket := range bucketsConfig.Buckets {
		newBucket, err := createBucketConfig(&bucket, streamsConfig)
		if err != nil {
			return nil, err
		}
		mappedConfig.Buckets[bucket.Id] = newBucket
	}
	if bucketsConfig.DefaultBucket != nil {
		defaultBucket, err := createBucketConfig(bucketsConfig.DefaultBucket, streamsConfig)
		if err != nil {
			return nil, err
		}
		mappedConfig.DefaultBucket = defaultBucket
	}
	return &mappedConfig, nil
}
func LoadBucketsConfigFromEnv(envConfig EnvConfig) (*MappedConfig, error) {

	log.Info("Loading buckets file located at ", envConfig.BucketsFile)
	configFileData, err := os.ReadFile(envConfig.BucketsFile)
	if err != nil {
		return nil, fmt.Errorf("could not read buckets config file at %s -> %s", envConfig.BucketsFile, err)
	}
	return LoadBucketsConfigFromString(string(configFileData))
}

func createBucketConfig(bucketConfig *jsonBucketConfig, streamsConfig map[string]StreamConfig) (*BucketConfig, error) {
	bucketStreamConfigs := make(map[string]StreamConfig)
	for _, strmCfgId := range bucketConfig.Streams {
		if strmCfg, ok := streamsConfig[strmCfgId]; ok {
			bucketStreamConfigs[strmCfgId] = strmCfg
		}
	}
	if len(bucketStreamConfigs) == 0 {
		return nil, fmt.Errorf("no streams defined for bucket")
	}
	return &BucketConfig{
		StreamConfigs: bucketStreamConfigs,
		Batch:         bucketConfig.Batch,
		BatchTimeout:  int64(bucketConfig.BatchTimeout),
		BucketId:      bucketConfig.Id,
	}, nil
}
