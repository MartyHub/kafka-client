package kafka_client

import (
	"os"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

const (
	adminTimeout      = 1 * time.Second
	maxPollIntervalMs = 3_000
	messageCount      = 50
	waitTimeoutMs     = 50
)

func TestMain(m *testing.M) {
	setupTestsLog()
	os.Exit(m.Run())
}

func setupTestsLog() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	zerolog.TimeFieldFormat = time.RFC3339Nano

	cw := zerolog.ConsoleWriter{Out: os.Stderr}
	cw.TimeFormat = "15:04:05.000"

	log.Logger = log.Output(cw)
}

func newTestAdmin(t *testing.T, client Client) Admin {
	result, err := client.NewAdmin(adminTimeout)
	require.NoError(t, err)

	return result
}

func newClientLocalCluster(t *testing.T) Client {
	client := NewClient(
		"localhost:9092,localhost:9093",
		WithLogLevel(LogLevelWarning),
	)
	a := newTestAdmin(t, client)
	defer a.Close()

	if err := a.NewTopicBuilder(t.Name()).WithPartitions(2).WithReplica(2).Create(); err != nil {
		if ke, ok := err.(kafka.Error); ok && ke.Code() != kafka.ErrTopicAlreadyExists {
			t.Fatal(ke)
		}
	}

	t.Cleanup(func() {
		deleteTestTopic(t, client)
	})

	return client
}

func deleteTestTopic(t *testing.T, client Client) {
	a := newTestAdmin(t, client)
	defer a.Close()

	if err := a.DeleteTopic(t.Name()); err != nil {
		t.Logf("[WARN] Failed to delete topic %s: %v", t.Name(), err)
	}
}
