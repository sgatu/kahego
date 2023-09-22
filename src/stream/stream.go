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
type PersistMessage struct {
	Message *Message
	Stream  string
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
	Init() error
	InBackupMode() bool
	Push(msg *Message) error
	Len() uint32
	Flush() error
	Close() error
}
type KafkaStream struct {
	kafkaConn    *kafka.Producer
	kafkaConfig  *kafka.ConfigMap
	topic        string
	deliveryChan chan kafka.Event
	inBackupMode bool
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
func (stream *KafkaStream) InBackupMode() bool {
	return stream.inBackupMode
}
func (stream *KafkaStream) Close() error {
	for {
		pending := stream.kafkaConn.Flush(300)
		if pending == 0 {
			stream.kafkaConn.Close()
		}
		close(stream.deliveryChan)
	}
}

func (stream *KafkaStream) deliveryManagement() {
	stream.inBackupMode = false
	for {
		ev, more := <-stream.deliveryChan
		if !more {
			return
		}
		m, ok := ev.(*kafka.Message)
		if ok && m.TopicPartition.Error != nil {
			fmt.Println("Could not write to kafka, switching to backup if any configured")
			stream.inBackupMode = true
			stream.kafkaConn.Close()
			go stream.waitForConnection()
			return
		}
	}
}
func (stream *KafkaStream) initProducer() bool {
	kafkaConn, err := kafka.NewProducer(stream.kafkaConfig)
	if err != nil {
		return false
	}
	stream.kafkaConn = kafkaConn
	return true
}
func (stream *KafkaStream) waitForConnection() {
	stream.inBackupMode = true
	for {
		if stream.initProducer() {
			go stream.deliveryManagement()
			return
		}
		//check connection every 5 seconds
		<-time.After(5000 * time.Second)
	}
}
func (stream *KafkaStream) Init() error {
	stream.deliveryChan = make(chan kafka.Event, 10000)
	if stream.initProducer() {

		go stream.deliveryManagement()
	} else {
		stream.inBackupMode = true
		go stream.waitForConnection()
	}
	return nil
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
	hasBackup    bool
	inBackupMode bool
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
			if stream.hasBackup {
				stream.inBackupMode = true
				return nil
			}
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
func (stream *FileStream) Init() error {
	stream.inBackupMode = false
	err := stream.rotateFile()
	if err != nil {
		return err
	}
	return nil
}

/*
Factory methods
*/
func getKafkaStream(streamConfig config.StreamConfig) (*KafkaStream, error) {

	configsParsed := kafka.StringMapToConfigEntries(streamConfig.Settings, kafka.AlterOperationSet)
	cfgMap := kafka.ConfigMap{}
	for _, cfg := range configsParsed {
		cfgMap.SetKey(cfg.Name, cfg.Value)
	}
	stream := KafkaStream{kafkaConfig: &cfgMap, topic: streamConfig.Key}
	return &stream, nil
}
func getFileStream(streamConfig config.StreamConfig) (*FileStream, error) {
	path, ok := streamConfig.Settings["path"]
	if !ok {
		return nil, errors.New("no path defined for fileStream")
	}
	rotateLengthStr, ok := streamConfig.Settings["sizeRotate"]
	var rotateLength int32 = 1024 * 1024 * 100 //100 MB default file size
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
		hasBackup:    streamConfig.Backup != nil,
	}
	return fs, nil
}
func (stream *FileStream) InBackupMode() bool {
	return stream.inBackupMode
}
func GetStream(streamConfig config.StreamConfig, backupChannel chan interface{}) (Stream, error) {
	var strm Stream
	var err error
	switch streamConfig.Type {
	case "kafka":
		strm, err = getKafkaStream(streamConfig)
	case "file":
		strm, err = getFileStream(streamConfig)
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
