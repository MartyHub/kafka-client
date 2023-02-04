package kafka_client

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog/log"
)

type Strategy int

func (s Strategy) String() string {
	switch s {
	case AtLeastOnceStrategy:
		return "AtLeastOnce"
	case AtMostOnceStrategy:
		return "AtMostOnce"
	}

	return "Unknown"
}

const (
	AtLeastOnceStrategy Strategy = iota
	AtMostOnceStrategy
)

type ConsumerCallback interface {
	Fatal(c Consumer, err error)
	Handle(c Consumer, m *kafka.Message) error
}

type Consumer struct {
	Strategy
	cc  ConsumerCallback
	kc  *kafka.Consumer
	run *bool
}

func newConsumer(config kafka.ConfigMap, topic string, cc ConsumerCallback, strategy Strategy) (Consumer, error) {
	log.Info().Msgf("Consumer config: %v", config)
	log.Info().Msgf("Creating consumer for topic %s with strategy %v...", topic, strategy)
	kc, err := kafka.NewConsumer(&config)

	if err != nil {
		return Consumer{}, err
	}

	if err = kc.Subscribe(topic, nil); err != nil {
		return Consumer{}, err
	}

	return Consumer{
		Strategy: strategy,
		cc:       cc,
		kc:       kc,
		run:      new(bool),
	}, nil
}

func (c Consumer) Start(pollTimeoutMs int) {
	log.Info().Msgf("Starting %v polling every %d ms...", c, pollTimeoutMs)

	sysChan := make(chan os.Signal, 1)
	signal.Notify(sysChan, syscall.SIGINT, syscall.SIGTERM)

	*c.run = true

	for *c.run {
		select {
		case sig := <-sysChan:
			log.Info().Msgf("%v receive signal %v", c, sig)
			c.Stop()
		default:
			e := c.kc.Poll(pollTimeoutMs)

			if e == nil {
				continue
			}

			switch ke := e.(type) {
			case *kafka.Message:
				log.Debug().Msgf("Received message: %v", ke)

				switch c.Strategy {
				case AtLeastOnceStrategy:
					c.atLeastOnce(ke)
				case AtMostOnceStrategy:
					c.atMostOnce(ke)
				}
			case kafka.Error:
				log.Warn().Msgf("Failed to poll: %v", e)
			}
		}
	}

	c.close()
}

func (c Consumer) Stop() {
	log.Info().Msgf("Stopping %v", c)
	*c.run = false
}

func (c Consumer) String() string {
	return fmt.Sprintf("Consumer %v", c.kc)
}

func (c Consumer) atLeastOnce(km *kafka.Message) {
	if c.handle(km) {
		c.storeMessage(km)
	}
}

func (c Consumer) atMostOnce(km *kafka.Message) {
	if c.storeMessage(km) {
		c.handle(km)
	}
}

func (c Consumer) handle(km *kafka.Message) bool {
	if err := c.cc.Handle(c, km); err != nil {
		log.Error().Err(err).Msgf("Message handling error")

		return false
	}

	return true
}

func (c Consumer) storeMessage(km *kafka.Message) bool {
	log.Debug().Msgf("Committing offset %v...", km.TopicPartition.Offset)
	tp, err := c.kc.StoreMessage(km)

	if err != nil {
		log.Error().Err(err).Msgf("Failed to commit offset")
		c.cc.Fatal(c, err)

		return false
	} else if tp[0].Error != nil {
		log.Error().Err(tp[0].Error).Msgf("Failed to commit offset")
		c.cc.Fatal(c, tp[0].Error)

		return false
	}

	return true
}

func (c Consumer) close() {
	log.Info().Msgf("Closing %v...", c)
	_ = c.kc.Close() // error is always nil
}
