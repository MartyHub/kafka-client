package kafka_client

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog/log"
)

type ProducerCallback interface {
	Delivered(m *kafka.Message)
	Fatal(p Producer, err error)
}

type Producer struct {
	kp    *kafka.Producer
	pc    ProducerCallback
	topic string
}

func newProducer(config kafka.ConfigMap, topic string, pc ProducerCallback) (Producer, error) {
	log.Info().Msgf("Creating producer for topic %s...", topic)
	kp, err := kafka.NewProducer(&config)

	if err != nil {
		return Producer{}, err
	}

	result := Producer{
		kp:    kp,
		pc:    pc,
		topic: topic,
	}

	go result.listen()

	log.Info().Msgf("Created %v", result)

	return result, nil
}

func (p Producer) fatal(err error) {
	log.Error().Err(err).Msgf("Delivery failed")
	p.pc.Fatal(p, err)
}

func (p Producer) listen() {
	for e := range p.kp.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				p.fatal(ev.TopicPartition.Error)
			} else {
				log.Debug().Msgf("Delivery successful: %v", ev)
				p.pc.Delivered(ev)
			}
		case kafka.Error:
			if ev.IsFatal() {
				p.fatal(ev)
			} else {
				log.Warn().Msgf("Delivery failed: %v", ev)
			}
		}
	}
}

func (p Producer) NewMessageBuilder(value []byte) MessageBuilder {
	return newMessageBuilder(p, value)
}

func (p Producer) Produce(message *kafka.Message) error {
	log.Debug().Msgf("Producing message %v...", message)

	return p.kp.Produce(message, nil)
}

func (p Producer) Flush(timeoutMs int) int {
	log.Debug().Msgf("Flushing %v...", p)
	return p.kp.Flush(timeoutMs)
}

func (p Producer) FlushAll(timeoutMs int) {
	for p.Flush(timeoutMs) > 0 {
	}
}

func (p Producer) Close() {
	log.Info().Msgf("Closing %v...", p)
	p.kp.Close()
}

func (p Producer) String() string {
	return fmt.Sprintf("Producer %v", p.kp)
}
