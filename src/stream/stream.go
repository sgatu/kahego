package stream

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
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
	data := make([]byte, 0, 4+totalLen)
	data = binary.LittleEndian.AppendUint32(data, uint32(totalLen))
	data = append(data, byte(len(msg.Bucket)))
	data = append(data, []byte(msg.Bucket)...)
	data = append(data, byte(len(msg.Key)))
	data = append(data, []byte(msg.Key)...)
	data = append(data, msg.Data...)
	return data
}

type Stream interface {
	Push(msg *Message) error
	Len() uint32
	Flush() error
	Close() error
}
type BackedUpStream interface {
	PushBackup(msg *Message) error
	getBackupStream() Stream
}
type KafkaStream struct {
	kafkaConn    *kafka.Producer
	kafkaConfig  *kafka.ConfigMap
	topic        string
	deliveryChan chan kafka.Event
	backup       Stream
}

func (stream *KafkaStream) Push(msg *Message) error {
	stream.kafkaConn.Produce(
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &stream.topic, Partition: kafka.PartitionAny},
			Key:            []byte(msg.Key),
			Value:          msg.Data,
			Timestamp:      time.Now(),
			TimestampType:  kafka.TimestampLogAppendTime,
		},
		stream.deliveryChan,
	)
	return nil
}
func (stream *KafkaStream) Flush() error {
	stream.kafkaConn.Flush(300)
	return nil
}
func (stream *KafkaStream) Len() uint32 {
	return uint32(stream.kafkaConn.Len())
}
func (stream *KafkaStream) Close() error {
	for {
		pending := stream.kafkaConn.Flush(300)
		if pending == 0 {
			stream.kafkaConn.Close()
		}
	}
}
func (stream *KafkaStream) PushBackup(msg *Message) error {
	if stream.getBackupStream() != nil {
		return stream.getBackupStream().Push(msg)
	}
	return nil
}
func (stream *KafkaStream) getBackupStream() Stream {
	return stream.backup
}

/*
FileStream used to write data to file
*/
type FileStream struct {
	path         string
	fileName     string
	file         *os.File
	queue        datastructures.Queue[*Message]
	writtenBytes int32
	rotateLength int32
}

func (stream *FileStream) Push(msg *Message) error {
	stream.queue.Push(&datastructures.Node[*Message]{Value: msg})
	return nil
}
func (stream *FileStream) rotateFile() error {
	if stream.writtenBytes >= stream.rotateLength || stream.file == nil {
		stream.writtenBytes = 0
		if stream.file != nil {
			stream.file.Sync()
			stream.file.Close()
		}
		fullPath := stream.path + stream.fileName + "-" + fmt.Sprintf("%d", time.Now().Unix())
		file, err := os.OpenFile(fullPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		stream.file = file
	}
	return nil

}
func (stream *FileStream) Flush() error {
	err := stream.rotateFile()
	if err != nil {
		return err
	}
	length := stream.queue.Len()
	for i := 0; i < int(length); i++ {
		node, err := stream.queue.Pop()
		if err == nil {
			serializedData := node.Value.Serialize()
			if _, err := stream.file.Write(serializedData); err != nil {
				stream.file.Sync()
				return err
			}
			stream.writtenBytes += int32(len(serializedData))
		} else {
			break
		}
	}
	return stream.file.Sync()
}
func (stream *FileStream) Len() uint32 {
	return stream.queue.Len()
}
func (stream *FileStream) Close() error {
	if stream.file != nil {
		stream.file.Close()
	}
	return nil
}
func getKafkaStream(streamConfig config.StreamConfig) (*KafkaStream, error) {
	
	kafkaConfigEntries := kafka.StringMapToConfigEntries(streamConfig.Settings, kafka.AlterOperationSet)
	kafkaConfigMap := kafka.
	return &KafkaStream{}, nil
}
func getFileStream(streamConfig config.StreamConfig) (*FileStream, error) {
	path, ok := streamConfig.Settings["path"]
	if !ok {
		return nil, errors.New("no path defined for fileStream")
	}
	rotateLengthStr, ok := streamConfig.Settings["sizeRotate"]
	var rotateLength int32 = 1000000
	if ok {
		fmt.Println("Found sizeRotate in config", rotateLengthStr)
		i, err := strconv.ParseInt(rotateLengthStr, 10, 32)
		if err == nil {
			fmt.Println("Parsed sizeRotate to", i)
			rotateLength = int32(i)
		} else {
			fmt.Println(err)
		}
	}
	fs := &FileStream{
		path:         path,
		file:         nil,
		writtenBytes: 0,
		rotateLength: rotateLength,
		fileName:     streamConfig.Key,
	}
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
