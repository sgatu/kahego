package stream

import (
	"fmt"

	"sgatu.com/kahego/src/config"
)

type Bucket struct {
	streams map[string]Stream
	id      string
}
type Stream interface {
	push() error
}
type KafkaStream struct {
}

func (stream *KafkaStream) push() error {
	return nil
}

func getKafkaStream(streamConfig config.StreamConfig) *KafkaStream {
	return &KafkaStream{}
}
func GetStream(streamConfig config.StreamConfig) (Stream, error) {
	if streamConfig.Type == "kafka" {
		return getKafkaStream(streamConfig), nil
	}
	return nil, fmt.Errorf("invalid stream type %s or not implemented", streamConfig.Type)
}
