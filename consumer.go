package kafka_client

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog/log"
)

type ConsumerCallback interface {
	Fatal(c Consumer, err error)
	Handle(c Consumer, m *kafka.Message) error
}

type Consumer struct {
	cc                 ConsumerCallback
	kc                 *kafka.Consumer
	mcs                map[string]chan *kafka.Message
	messageWaitTimeout time.Duration
	run                *bool
}

func newConsumer(config kafka.ConfigMap, topic string, cc ConsumerCallback) (Consumer, error) {
	log.Info().Msgf("Consumer config: %v", config)
	log.Info().Msgf("Creating consumer for topic %s...", topic)
	kc, err := kafka.NewConsumer(&config)

	if err != nil {
		return Consumer{}, err
	}

	maxPollIntervalMs, err := config.Get(MaxPollIntervalMs, 300_000)

	if err != nil {
		return Consumer{}, err
	}

	messageWaitTimeout := time.Duration(maxPollIntervalMs.(int)*75/100) * time.Millisecond

	log.Info().Msgf("Consumer message wait timeout: %v", messageWaitTimeout)

	consumer := Consumer{
		cc:                 cc,
		kc:                 kc,
		messageWaitTimeout: messageWaitTimeout,
		mcs:                make(map[string]chan *kafka.Message),
		run:                new(bool),
	}

	if err := consumer.subscribe(topic); err != nil {
		return consumer, err
	}

	return consumer, nil
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
				c.message(ke)
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

func (c Consumer) subscribe(topic string) error {
	return c.kc.Subscribe(topic, func(kc *kafka.Consumer, e kafka.Event) error {
		switch ke := e.(type) {
		case kafka.AssignedPartitions:
			for _, tp := range ke.Partitions {
				c.assignedPartition(tp)
			}
		case kafka.RevokedPartitions:
			for _, tp := range ke.Partitions {
				c.revokedPartition(tp)
			}
		}

		return nil
	})
}

func (c Consumer) assignedPartition(tp kafka.TopicPartition) {
	log.Info().Msgf("Assigned partition: %v", tp)

	channel := make(chan *kafka.Message)
	c.mcs[c.channelKey(tp)] = channel

	go func() {
		for {
			message, more := <-channel

			if more {
				c.handle(message)
				c.storeMessage(message)
			} else {
				break
			}
		}
	}()
}

func (c Consumer) revokedPartition(tp kafka.TopicPartition) {
	log.Info().Msgf("Revoked partition: %v", tp)

	key := c.channelKey(tp)

	close(c.mcs[key])
	delete(c.mcs, key)
}

func (c Consumer) handle(km *kafka.Message) {
	log.Debug().Msg("Handling message...")

	if err := c.cc.Handle(c, km); err != nil {
		log.Error().Err(err).Msgf("Message handling error")
	}
}

func (c Consumer) storeMessage(km *kafka.Message) {
	log.Debug().Msgf("Committing offset %v...", km.TopicPartition)
	tp, err := c.kc.StoreMessage(km)

	if err != nil {
		c.fatal(err)
	} else if tp[0].Error != nil {
		c.fatal(tp[0].Error)
	}
}

func (c Consumer) channelKey(tp kafka.TopicPartition) string {
	return fmt.Sprintf("%s[%d]", *tp.Topic, tp.Partition)
}

func (c Consumer) message(km *kafka.Message) {
	log.Debug().Msgf("Received message: %v", km)

	select {
	case c.mcs[c.channelKey(km.TopicPartition)] <- km:
	case <-time.After(c.messageWaitTimeout):
		c.wait(km)
	}
}

func (c Consumer) wait(km *kafka.Message) {
	log.Info().Msgf("Waiting for slow consumer on %v...", km.TopicPartition)

	if c.pause(km) {
		c.mcs[c.channelKey(km.TopicPartition)] <- km
		c.resume(km)
	}
}

func (c Consumer) pause(km *kafka.Message) bool {
	log.Info().Msgf("Pausing %v...", km.TopicPartition)

	if _, err := c.kc.Commit(); err != nil {
		c.fatal(err)

		return false
	}

	if err := c.kc.Pause([]kafka.TopicPartition{km.TopicPartition}); err != nil {
		c.fatal(err)

		return false
	}

	return true
}

func (c Consumer) resume(km *kafka.Message) {
	log.Info().Msgf("Resuming %v...", km.TopicPartition)

	if err := c.kc.Resume([]kafka.TopicPartition{km.TopicPartition}); err != nil {
		c.fatal(err)
	}
}

func (c Consumer) fatal(err error) {
	log.Error().Err(err)

	c.Stop()
	c.cc.Fatal(c, err)
}

func (c Consumer) close() {
	log.Info().Msgf("Closing %v...", c)
	_ = c.kc.Close() // error is always nil
}
