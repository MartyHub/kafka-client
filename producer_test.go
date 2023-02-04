package kafka_client

import (
	"strconv"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestingProducerCallback struct {
	count int
	t     *testing.T
}

func newTestingProducerCallback(t *testing.T) *TestingProducerCallback {
	return &TestingProducerCallback{
		t: t,
	}
}

func (pc *TestingProducerCallback) Fatal(_ Producer, err error) {
	pc.t.Fatal(err)
}

func (pc *TestingProducerCallback) Delivered(m *kafka.Message) {
	assert.Equal(pc.t, pc.t.Name(), *m.TopicPartition.Topic)
	assert.Equal(pc.t, pc.t.Name(), string(m.Value))

	key, err := strconv.Atoi(string(m.Key))
	assert.NoError(pc.t, err)

	assert.Equal(pc.t, pc.count, key)
	pc.count++
}

func produceTestMessages(t *testing.T, client Client) {
	pc := newTestingProducerCallback(t)
	producer, err := client.NewProducer(t.Name(), pc)
	require.NoError(t, err)
	defer producer.Close()

	for i := 0; i < messageCount; i++ {
		require.NoError(
			t,
			producer.NewMessageBuilder([]byte(t.Name())).
				AddHeader("key", []byte(t.Name())).
				WithKey([]byte(strconv.Itoa(i))).
				Produce(),
		)
	}

	producer.FlushAll(waitTimeoutMs)

	assert.Equal(t, messageCount, pc.count)
}

func TestProducer(t *testing.T) {
	client := newClientLocalCluster(t)

	produceTestMessages(t, client)
}
