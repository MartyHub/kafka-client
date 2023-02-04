package kafka_client

import (
	"context"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog/log"
)

type Admin struct {
	ka      *kafka.AdminClient
	timeout time.Duration
}

func newAdmin(config kafka.ConfigMap, timeout time.Duration) (Admin, error) {
	log.Info().Msg("Creating admin...")
	ka, err := kafka.NewAdminClient(&config)

	if err != nil {
		return Admin{}, err
	}

	log.Info().Msgf("Created admin %v", ka)

	return Admin{
		ka:      ka,
		timeout: timeout,
	}, nil
}

func (a Admin) NewTopicBuilder(name string) TopicBuilder {
	return newTopicBuilder(a, name)
}

func (a Admin) CreateTopic(specification kafka.TopicSpecification) error {
	log.Info().Msgf("Creating topic %s...", specification.Topic)

	result, err := a.ka.CreateTopics(
		context.Background(),
		[]kafka.TopicSpecification{specification},
		kafka.SetAdminOperationTimeout(a.timeout),
	)

	if err != nil {
		return err
	}

	return checkTopicResult(result[0])
}

func (a Admin) DeleteTopic(topic string) error {
	log.Info().Msgf("Deleting topic %s...", topic)

	result, err := a.ka.DeleteTopics(
		context.Background(),
		[]string{topic},
		kafka.SetAdminOperationTimeout(a.timeout),
	)

	if err != nil {
		return err
	}

	return checkTopicResult(result[0])
}

func (a Admin) Close() {
	log.Info().Msgf("Closing admin %v...", a)
	a.ka.Close()
}

func (a Admin) String() string {
	return a.ka.String()
}

func checkTopicResult(tr kafka.TopicResult) error {
	if tr.Error.Code() != kafka.ErrNoError {
		return tr.Error
	}

	log.Info().Msgf("Processed topic %s", tr.Topic)

	return nil
}
