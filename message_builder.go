package kafka_client

import "github.com/confluentinc/confluent-kafka-go/kafka"

type MessageBuilder struct {
	headers  []kafka.Header
	key      []byte
	producer Producer
	value    []byte
}

func newMessageBuilder(producer Producer, value []byte) MessageBuilder {
	return MessageBuilder{
		producer: producer,
		value:    value,
	}
}

func (mb MessageBuilder) Produce() error {
	return mb.producer.Produce(mb.build(mb.producer.topic))
}

func (mb MessageBuilder) AddHeader(key string, value []byte) MessageBuilder {
	mb.headers = append(mb.headers, kafka.Header{
		Key:   key,
		Value: value,
	})

	return mb
}

func (mb MessageBuilder) WithKey(key []byte) MessageBuilder {
	mb.key = key

	return mb
}

func (mb MessageBuilder) build(topic string) *kafka.Message {
	return &kafka.Message{
		Headers:        mb.headers,
		Key:            mb.key,
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          mb.value,
	}
}
