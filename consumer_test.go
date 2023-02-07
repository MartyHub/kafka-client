package kafka_client

import (
	"strconv"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestingConsumerCallback struct {
	keys map[int]bool
	t    *testing.T
}

func newTestingConsumerCallback(t *testing.T) *TestingConsumerCallback {
	return &TestingConsumerCallback{
		keys: make(map[int]bool),
		t:    t,
	}
}

func (cc *TestingConsumerCallback) Fatal(_ Consumer, err error) {
	cc.t.Fatal(err)
}

func (cc *TestingConsumerCallback) Handle(c Consumer, m *kafka.Message) error {
	assert.Equal(cc.t, cc.t.Name(), *m.TopicPartition.Topic)
	assert.Len(cc.t, m.Headers, 1)
	assert.Equal(cc.t, "key", m.Headers[0].Key)
	assert.Equal(cc.t, cc.t.Name(), string(m.Headers[0].Value))
	assert.Equal(cc.t, cc.t.Name(), string(m.Value))

	key, err := strconv.Atoi(string(m.Key))
	assert.NoError(cc.t, err)

	assert.False(cc.t, cc.keys[key])
	cc.keys[key] = true

	if len(cc.keys) == messageCount/2 {
		// simulate slow consumer
		time.Sleep(maxPollIntervalMs * 0.75 * time.Millisecond)
	}

	if len(cc.keys) == messageCount {
		c.Stop()
	}

	return nil
}

func TestConsumer(t *testing.T) {
	client := newClientLocalCluster(t)

	go produceTestMessages(t, client)

	cc := newTestingConsumerCallback(t)
	consumer, err := client.Configure(
		WithConsumerGroupId(t.Name()),
		WithConsumerHeartbeatIntervalMs(maxPollIntervalMs/2),
		WithConsumerMaxPollIntervalMs(maxPollIntervalMs),
		WithConsumerSessionTimeoutMs(maxPollIntervalMs),
	).
		NewConsumer(t.Name(), cc)
	require.NoError(t, err)

	consumer.Start(waitTimeoutMs)
	assert.Equal(t, messageCount, len(cc.keys))
}
