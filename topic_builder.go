package kafka_client

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type TopicBuilder struct {
	admin      Admin
	config     map[string]string
	name       string
	partitions int
	replica    int
}

func newTopicBuilder(admin Admin, name string) TopicBuilder {
	return TopicBuilder{
		admin:      admin,
		name:       name,
		partitions: 24,
		replica:    3,
	}
}

func (tb TopicBuilder) Create() error {
	return tb.admin.CreateTopic(tb.build())
}

func (tb TopicBuilder) WithConfig(key, value string) TopicBuilder {
	tb.config[key] = value

	return tb
}

func (tb TopicBuilder) WithPartitions(partitions int) TopicBuilder {
	tb.partitions = partitions

	return tb
}

func (tb TopicBuilder) WithReplica(replica int) TopicBuilder {
	tb.replica = replica

	return tb
}

func (tb TopicBuilder) build() kafka.TopicSpecification {
	return kafka.TopicSpecification{
		Topic:             tb.name,
		NumPartitions:     tb.partitions,
		ReplicationFactor: tb.replica,
		Config:            tb.config,
	}
}
