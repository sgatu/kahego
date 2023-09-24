package stream

import (
	"errors"
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
	hasError     bool
	lastErr      error
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
		stream.hasError = true
		stream.lastErr = err
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
	for {
		if stream.kafkaConn.Len() > 0 {
			pending := stream.kafkaConn.Flush(300)
			if pending == 0 {
				stream.kafkaConn.Close()
			}
		}
		close(stream.deliveryChan)
	}
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
				stream.lastErr = errors.New("could not write to kafka. err: " + m.TopicPartition.Error.Error())
				stream.hasError = true
				stream.kafkaConn.Close()
				return
			}
		case ev2 := <-stream.kafkaConn.Events():
			if err, ok := ev2.(kafka.Error); ok {
				if err.Code() == kafka.ErrResolve || err.Code() == kafka.ErrAllBrokersDown || err.Code() == kafka.ErrNetworkException {
					stream.lastErr = errors.New("could not write to kafka. err: " + err.Error())
					stream.hasError = true
					stream.kafkaConn.Close()
					return
				}
			}
		}
	}
}
func (stream *KafkaStream) initProducer() error {
	kafkaConn, err := kafka.NewProducer(stream.kafkaConfig)
	if err != nil {
		return err
	}
	stream.kafkaConn = kafkaConn
	return nil
}
func (stream *KafkaStream) Init() error {
	if stream.deliveryChan == nil {
		stream.deliveryChan = make(chan kafka.Event, 10000)
	}
	if stream.kafkaConn == nil || stream.hasError {
		if err := stream.initProducer(); err != nil {
			stream.hasError = true
			stream.lastErr = errors.New("could not initialize kafka connection due to: " + err.Error())
			return stream.lastErr
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
