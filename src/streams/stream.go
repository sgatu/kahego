package streams

import (
	"encoding/binary"
	"errors"
	"fmt"

	linkedlist "github.com/sgatu/go-simple-linked-list"
	"sgatu.com/kahego/src/config"
)

type Message struct {
	Bucket string
	Key    string
	Data   []byte
}
type MessageError struct {
	Key  string
	Data []byte
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
		key = string(msg[buckNameLen+2 : buckNameLen+keyLen+2])
	}
	data := msg[buckNameLen+keyLen+2:]
	return &Message{
		Bucket: bucketName,
		Key:    key,
		Data:   data,
	}, nil
}

func (msg *Message) Serialize(includeLen bool) []byte {
	totalLen := len(msg.Data) + len(msg.Key) + len(msg.Bucket) + 2
	var data []byte
	if includeLen {
		data = make([]byte, 0, 4+totalLen)
		data = binary.LittleEndian.AppendUint32(data, uint32(totalLen))
	} else {
		data = make([]byte, 0, totalLen)
	}
	data = append(data, byte(len(msg.Bucket)))
	data = append(data, []byte(msg.Bucket)...)
	data = append(data, byte(len(msg.Key)))
	data = append(data, []byte(msg.Key)...)
	data = append(data, msg.Data...)
	return data
}

type Stream interface {
	Init() error
	HasError() bool
	GetError() error
	Push(msg *Message) error
	Len() uint32
	Flush() error
	Close() error
	GetQueue() *linkedlist.LinkedList[*Message]
}

func GetStream(streamConfig config.StreamConfig, bucket string) (Stream, error) {
	var strm Stream
	var err error
	switch streamConfig.Type {
	case "kafka":
		strm, err = getKafkaStream(streamConfig, bucket)
	case "file":
		strm, err = getFileStream(streamConfig, bucket)
	default:
		return nil, fmt.Errorf("invalid stream type %s or not implemented", streamConfig.Type)
	}
	if err == nil {
		err := strm.Init()
		if err != nil {
			return nil, err
		}
	}
	return strm, err
}
