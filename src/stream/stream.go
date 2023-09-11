package stream

import (
	"fmt"
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

func getKafkaStream(m map[string]any) *KafkaStream {
	return &KafkaStream{}
}
func GetStream(m map[string]interface{}) (Stream, error) {
	if m["type"] == "kafka" {
		return getKafkaStream(m), nil
	}
	return nil, fmt.Errorf("invalid stream type %s or not implemented", m["type"])
}
