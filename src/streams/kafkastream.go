package streams

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"sgatu.com/kahego/src/config"
	"sgatu.com/kahego/src/datastructures"
)

type KafkaErrorType string

const (
	ConnectionError KafkaErrorType = "ConnectionError"
	DeliveryError   KafkaErrorType = "DeliveryError"
)

type KafkaStreamError struct {
	errorType KafkaErrorType
	err       error
}

func (kse *KafkaStreamError) Error() string {
	return fmt.Sprintf("Type: %s, Error: %s", kse.errorType, kse.err)
}
func (kse *KafkaStreamError) GetType() KafkaErrorType {
	return kse.errorType
}

type KafkaStream struct {
	kafkaConn    *kafka.Producer
	kafkaConfig  *kafka.ConfigMap
	queue        datastructures.Queue[*Message]
	topic        string
	deliveryChan chan kafka.Event
	lastErr      *KafkaStreamError
	errorLock    sync.Mutex
}

func (stream *KafkaStream) setErrorMode(_type KafkaErrorType, err error) {
	kafkaError := &KafkaStreamError{err: err, errorType: _type}
	fmt.Printf("Kafka Stream Error: %s\n", kafkaError)
	stream.errorLock.Lock()
	defer stream.errorLock.Unlock()
	stream.lastErr = kafkaError

}
func (stream *KafkaStream) Push(msg *Message) error {
	stream.queue.Push(&datastructures.Node[*Message]{Value: msg})
	err := stream.kafkaConn.Produce(
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &stream.topic, Partition: kafka.PartitionAny},
			Key:            []byte(msg.Key),
			Value:          msg.Data,
			Timestamp:      time.Now(),
			TimestampType:  kafka.TimestampLogAppendTime,
		},
		stream.deliveryChan,
	)
	if err != nil {
		stream.setErrorMode(DeliveryError, err)
	}
	return err
}
func (stream *KafkaStream) Flush() error {
	stream.kafkaConn.Flush(300)
	stream.queue.Clear()
	return nil
}
func (stream *KafkaStream) Len() uint32 {
	return uint32(stream.kafkaConn.Len())
}
func (stream *KafkaStream) Close() error {
	if !stream.HasError() && stream.kafkaConn.Len() > 0 {
		for {
			pending := stream.kafkaConn.Flush(300)
			if pending == 0 {
				stream.kafkaConn.Close()
				break
			}
		}
	}
	close(stream.deliveryChan)
	stream.deliveryChan = nil
	return nil
}

func (stream *KafkaStream) deliveryManagement() {
	stream.lastErr = nil
	for {
		select {
		case deliveryEvent, more := <-stream.deliveryChan:
			if !more {
				return
			}
			m, ok := deliveryEvent.(*kafka.Message)
			if ok && m.TopicPartition.Error != nil {
				stream.setErrorMode(DeliveryError, errors.New("could not write to kafka. err: "+m.TopicPartition.Error.Error()))
				stream.kafkaConn.Close()
				return
			}
		case connectionEvent := <-stream.kafkaConn.Events():
			if err, ok := connectionEvent.(kafka.Error); ok {
				if err.Code() == kafka.ErrResolve || err.Code() == kafka.ErrAllBrokersDown || err.Code() == kafka.ErrNetworkException {
					stream.setErrorMode(ConnectionError, errors.New("could not write to kafka. err: "+err.Error()))
					stream.kafkaConn.Close()
					return
				}
			}
		}
	}

}
func (stream *KafkaStream) testBrokers() error {
	brokersCfgVal, err := stream.kafkaConfig.Get("bootstrap.servers", "")
	if err == nil {
		brokers := fmt.Sprintf("%s", brokersCfgVal)
		split := strings.Split(brokers, ",")
		allError := true
		for _, broker := range split {
			d := net.Dialer{Timeout: 2 * time.Second}
			conn, err := d.Dial("tcp", strings.TrimSpace(broker))
			if err == nil {
				conn.Close()
				allError = false
				break
			}
		}
		if !allError {
			return nil
		}
		return errors.New("could not connect to any brokers")
	}
	return err
}
func (stream *KafkaStream) initProducer() error {
	err := stream.testBrokers()
	if err != nil {
		return err
	}
	kafkaConn, err := kafka.NewProducer(stream.kafkaConfig)
	if err != nil {
		return err
	}
	stream.kafkaConn = kafkaConn
	return nil
}
func (stream *KafkaStream) Init() error {
	stream.errorLock = sync.Mutex{}
	if err := stream.initProducer(); err != nil {
		stream.setErrorMode(ConnectionError, errors.New("could not initialize kafka connection due to: "+err.Error()))
		return stream.GetError()
	}
	if stream.deliveryChan == nil {
		stream.deliveryChan = make(chan kafka.Event, 10000)
	}
	go stream.deliveryManagement()
	return nil
}

func (stream *KafkaStream) GetQueue() *datastructures.Queue[*Message] {
	return &stream.queue
}
func (stream *KafkaStream) GetError() error {
	stream.errorLock.Lock()
	defer stream.errorLock.Unlock()
	return stream.lastErr
}
func (stream *KafkaStream) HasError() bool {
	stream.errorLock.Lock()
	defer stream.errorLock.Unlock()
	return stream.lastErr != nil
}

/*
Factory method
*/
func getKafkaStream(streamConfig config.StreamConfig, bucketName string) (*KafkaStream, error) {

	configsParsed := kafka.StringMapToConfigEntries(streamConfig.Settings, kafka.AlterOperationSet)
	cfgMap := kafka.ConfigMap{}
	for _, cfg := range configsParsed {
		cfgMap.SetKey(cfg.Name, cfg.Value)
	}
	stream := KafkaStream{kafkaConfig: &cfgMap, topic: bucketName}
	return &stream, nil
}
