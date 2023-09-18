package stream

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"sgatu.com/kahego/src/config"
	"sgatu.com/kahego/src/datastructures"
)

type Message struct {
	Bucket string
	Key    string
	Data   []byte
}

func GetMessage(msg []byte) (*Message, error) {
	if len(msg) < 1 {
		return nil, errors.New("invalid message, could not deserialize")
	}
	buckNameLen := msg[0]
	if len(msg) < int(buckNameLen)+1 {
		return nil, errors.New("invalid message, could not deserialize")
	}
	bucketName := string(msg[1 : buckNameLen+1])
	keyLen := msg[buckNameLen+1]
	if len(msg) < int(buckNameLen+keyLen)+2 {
		return nil, errors.New("invalid message, could not deserialize")
	}
	var key string = ""
	if keyLen > 0 {
		key = string(msg[buckNameLen+1 : buckNameLen+keyLen+2])
	}
	data := msg[buckNameLen+keyLen+2:]
	return &Message{
		Bucket: bucketName,
		Key:    key,
		Data:   data,
	}, nil

}
func (msg *Message) Serialize() []byte {
	totalLen := len(msg.Data) + len(msg.Key) + len(msg.Bucket) + 2
	data := make([]byte, 4+totalLen)
	data = binary.LittleEndian.AppendUint32(data, uint32(totalLen))
	data = append(data, byte(len(msg.Bucket)))
	data = append(data, []byte(msg.Bucket)...)
	data = append(data, byte(len(msg.Key)))
	data = append(data, []byte(msg.Key)...)
	data = append(data, msg.Data...)
	return data
}

type Bucket struct {
	Streams map[string]Stream
	Id      string
	Batch   int32
}
type Stream interface {
	Push(msg Message) error
	Len() uint32
	Flush() error
}
type KafkaStream struct {
}
type FileStream struct {
	path  string
	file  *os.File
	queue datastructures.Queue[Message]
}

func (stream *KafkaStream) Push(msg Message) error {
	return nil
}
func (stream *KafkaStream) Flush() error {
	return nil
}
func (stream *KafkaStream) Len() uint32 {
	return 0
}
func (stream *FileStream) Push(msg Message) error {
	stream.queue.Push(&datastructures.Node[Message]{Value: msg})
	return nil
}
func (stream *FileStream) Flush() error {
	len := stream.queue.Len()
	for i := 0; i < int(len); i++ {
		node, err := stream.queue.Pop()
		if err != nil {
			if _, err := stream.file.Write(node.Value.Serialize()); err != nil {
				stream.file.Sync()
				return err
			}
		} else {
			break
		}
	}
	return stream.file.Sync()
}
func (stream *FileStream) Len() uint32 {
	return stream.queue.Len()
}

func getKafkaStream(streamConfig config.StreamConfig) (*KafkaStream, error) {
	return &KafkaStream{}, nil
}
func getFileStream(streamConfig config.StreamConfig) (*FileStream, error) {
	fs := &FileStream{
		path: streamConfig.Settings["path"],
	}
	file, err := os.OpenFile(fs.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	fs.file = file
	return fs, nil
}
func GetStream(streamConfig config.StreamConfig) (Stream, error) {
	switch streamConfig.Type {
	case "kafka":
		return getKafkaStream(streamConfig)
	case "file":
		return getFileStream(streamConfig)
	default:
		return nil, fmt.Errorf("invalid stream type %s or not implemented", streamConfig.Type)
	}
}
