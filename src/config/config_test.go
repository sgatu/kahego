package config

import (
	"math"
	"testing"
)

func Test_bad_configs(t *testing.T) {
	bad_configs := []string{
		"{}",
		`{
			"streamsConfigs":{}
			"buckets":[]
			"defaultBucket:{}
		}`,
		`{
			"streamsConfigs":{
				"stream_1": {
					"type":"file",
					"settings": {
						"path":"/tmp"
					}
				}
			},
			"buckets": [
				{
					"id": "no_stream_bucket",
					"batch": 10000,
					"streams": [],
					"batchTimeout": 30000
				}
			]
		}
		`,
		`{
			"streamsConfigs":{
				"stream_1": {
					"type":"file",
					"settings": {
						"path":"/tmp"
					}
				}
			},
			"buckets": [
				{
					"id": "wrong_stream_id_bucket",
					"batch": 10000,
					"streams": ["stream_2"],
					"batchTimeout": 30000
				}
			]
		}
		`,
	}
	for pos, bad_config := range bad_configs {
		mappedConfig, err := LoadBucketsConfigFromString(bad_config)
		if err == nil {
			t.Errorf("LoadBucketsConfigFromString should have failed. Config at pos %d", pos)
		}
		if mappedConfig != nil {
			t.Errorf("LoadBucketsConfigFromString should have returned a nil config. Config at pos %d", pos)
		}
	}
}

func Test_good_config(t *testing.T) {
	bucketsConfig := `{
		"streamsConfigs":{
			"stream_1": {
				"type":"file",
				"slice": 0.5,
				"settings": {
					"path":"/tmp",
					"sizeRotate":"100MB",
					"fileNameTemplate":"{bucket}",
					"maxFiles": "10"
				}
			}
		},
		"buckets": [
			{
				"id": "requests",
				"batch": 10000,
				"streams": ["stream_1"],
				"batchTimeout": 30000
			}
		]
	}
	`
	mappedConfig, err := LoadBucketsConfigFromString(bucketsConfig)
	if err != nil {
		t.Errorf("LoadBucketsConfigFromString should not have failed. err: %s", err)
	}
	if mappedConfig == nil {
		t.Fatalf("LoadBucketsConfigFromString should have not returned a nil config.")
	}
	if len(mappedConfig.Buckets) != 1 {
		t.Fatal("Expected 1 bucket defined")
	}
	if len(mappedConfig.Buckets["requests"].StreamConfigs) != 1 {
		t.Fatal("Expected 1 stream defined for requests bucket")
	}
	stream, ok := mappedConfig.Buckets["requests"].StreamConfigs["stream_1"]
	if !ok {
		t.Fatal("Wrong stream id in bucket")
	}
	if !withinTolerance(float64(stream.Slice), 0.5, 0.001) {
		t.Fatal("Parse error on stream slice")
	}
	if stream.Type != "file" {
		t.Fatal("Parse error on stream type")
	}
	if len(stream.Settings) != 4 {
		t.Fatal("Parse error on file stream settings")
	}
}

func withinTolerance(a, b, e float64) bool {
	return math.Abs(a-b) < e
}
