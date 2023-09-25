package stream

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"sgatu.com/kahego/src/config"
	"sgatu.com/kahego/src/datastructures"
)

type KafkaStream struct {
	kafkaConn    *kafka.Producer
	kafkaConfig  *kafka.ConfigMap
	queue        datastructures.Queue[*Message]
	topic        string
	deliveryChan chan kafka.Event
	slice        float32
	hasError     bool
	lastErr      error
	id           int
}

func (stream *KafkaStream) setErrorMode(err error) {
	fmt.Printf("Kafka Stream Error: %s\n", err)
	stream.hasError = true
	stream.lastErr = err
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
		stream.setErrorMode(err)
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
func (stream *KafkaStream) HasError() bool {
	return stream.hasError
}
func (stream *KafkaStream) Close() error {
	if !stream.hasError && stream.kafkaConn.Len() > 0 {
		for {
			pending := stream.kafkaConn.Flush(300)
			if pending == 0 {
				stream.kafkaConn.Close()
				break
			}
		}
	}
	fmt.Printf("Close deliveryChan %#v, %d\n", stream.deliveryChan, stream.id)
	close(stream.deliveryChan)
	stream.deliveryChan = nil
	return nil
}

func (stream *KafkaStream) deliveryManagement() {
	stream.hasError = false
	for {
		select {
		case ev, more := <-stream.deliveryChan:
			if !more {
				return
			}
			m, ok := ev.(*kafka.Message)
			if ok && m.TopicPartition.Error != nil {
				stream.setErrorMode(errors.New("could not write to kafka. err: " + m.TopicPartition.Error.Error()))
				stream.kafkaConn.Close()
				return
			}
		case ev2 := <-stream.kafkaConn.Events():
			if err, ok := ev2.(kafka.Error); ok {
				if err.Code() == kafka.ErrResolve || err.Code() == kafka.ErrAllBrokersDown || err.Code() == kafka.ErrNetworkException {
					stream.setErrorMode(errors.New("could not write to kafka. err: " + err.Error()))
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

	stream.id = rand.Int()
	if stream.kafkaConn == nil || stream.hasError {
		if err := stream.initProducer(); err != nil {
			stream.setErrorMode(errors.New("could not initialize kafka connection due to: " + err.Error()))
			return stream.lastErr
		}
		fmt.Printf("Check deliveryChan %#v, %d\n", stream.deliveryChan, stream.id)
		if stream.deliveryChan == nil {
			fmt.Printf("Initializing deliveryChan %#v, %d\n", stream.deliveryChan, stream.id)
			stream.deliveryChan = make(chan kafka.Event, 10000)
		}
		go stream.deliveryManagement()
	}
	return nil
}
func (stream *KafkaStream) GetError() error {
	return stream.lastErr
}
func (stream *KafkaStream) GetQueue() *datastructures.Queue[*Message] {
	return &stream.queue
}
func (stream *KafkaStream) GetSlice() float32 {
	return stream.slice
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
	stream := KafkaStream{kafkaConfig: &cfgMap, topic: streamConfig.Key, slice: streamConfig.Slice}
	return &stream, nil
}
