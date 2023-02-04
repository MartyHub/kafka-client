package kafka_client

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog/log"
)

const (
	AllowAutoCreateTopics = "allow.auto.create.topics"
	AutoOffsetReset       = "auto.offset.reset"
	BootstrapServers      = "bootstrap.servers"
	BrokerAddressFamily   = "broker.address.family"
	EnableAutoOffsetStore = "enable.auto.offset.store"
	GroupId               = "group.id"
	LogLevel              = "log_level"
)

const (
	AutoOffsetResetEarliest = "earliest"
	AutoOffsetResetLatest   = "latest"
)

const (
	LogLevelEmergency = iota
	LogLevelAlert
	LogLevelCritical
	LogLevelError
	LogLevelWarning
	LogLevelNotice
	LogLevelInfo
	LogLevelDebug
)

type Client struct {
	// see https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
	Config kafka.ConfigMap
}

type ClientConfigurer func(kafka.ConfigMap)

func WithConsumerAutoOffsetReset(value string) ClientConfigurer {
	return func(configMap kafka.ConfigMap) {
		configMap[AutoOffsetReset] = value
	}
}

func WithConsumerEnableAutoOffsetStore(value bool) ClientConfigurer {
	return func(configMap kafka.ConfigMap) {
		configMap[EnableAutoOffsetStore] = value
	}
}

func WithConsumerGroupId(value string) ClientConfigurer {
	return func(configMap kafka.ConfigMap) {
		configMap[GroupId] = value
	}
}

func WithLogLevel(value int) ClientConfigurer {
	return func(configMap kafka.ConfigMap) {
		configMap[LogLevel] = value
	}
}

func NewClient(servers string, configurers ...ClientConfigurer) Client {
	log.Info().Msgf("Creating Kafka client @ %s...", servers)

	defaultConfig := defaultConfig(servers)
	log.Info().Msgf("Default Config: %v", defaultConfig)
	result := Client{Config: defaultConfig}.Configure(configurers...)
	log.Info().Msgf("Final Config: %v", result.Config)
	log.Info().Msgf("Library version: %s", result.LibraryVersion())

	return result
}

func defaultConfig(servers string) kafka.ConfigMap {
	return kafka.ConfigMap{
		BootstrapServers:    servers,
		BrokerAddressFamily: "v4",
		LogLevel:            LogLevelWarning,
	}
}

func (c Client) Configure(configurers ...ClientConfigurer) Client {
	result := c.Clone()

	for _, configurer := range configurers {
		configurer(result.Config)
	}

	return result
}

func (c Client) Default(key string, value kafka.ConfigValue) Client {
	if c.HasConfig(key) {
		return c
	}

	result := c.Clone()

	result.Config[key] = value

	return result
}

func (c Client) NewAdmin(timeout time.Duration) (Admin, error) {
	return newAdmin(c.Config, timeout)
}

func (c Client) NewConsumer(topic string, cc ConsumerCallback, strategy Strategy) (Consumer, error) {
	config := c.Default(AllowAutoCreateTopics, false).
		Default(AutoOffsetReset, AutoOffsetResetEarliest).
		Default(EnableAutoOffsetStore, false).
		Config

	return newConsumer(config, topic, cc, strategy)
}

func (c Client) NewProducer(topic string, pc ProducerCallback) (Producer, error) {
	return newProducer(c.Config, topic, pc)
}

func (c Client) Clone() Client {
	config := make(map[string]kafka.ConfigValue, len(c.Config))

	for k, v := range c.Config {
		config[k] = v
	}

	return Client{
		Config: config,
	}
}

func (c Client) HasConfig(key string) bool {
	value, found := c.Config[key]

	return found && value != nil
}

func (c Client) LibraryVersion() string {
	_, result := kafka.LibraryVersion()

	return result
}

func (c Client) Servers() string {
	return c.Config[BootstrapServers].(string)
}

func (c Client) String() string {
	return fmt.Sprintf("Kafka client @ %s", c.Servers())
}
