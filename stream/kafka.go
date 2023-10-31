package stream

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func ProduceMessage(producer *kafka.Producer, topic string, item interface{}) error {
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
		Value:          []byte(fmt.Sprintf("%v", item)),
	}

	return producer.Produce(message, nil)
}

func CreateProducer(kafkaBroker string) (*kafka.Producer, error) {
	producerConfig := &kafka.ConfigMap{
		"bootstrap.servers":        kafkaBroker,
		"acks":                     "all",
		"compression.type":         "snappy",
		"batch.size":               16384,
		"linger.ms":                10,
		"max.request.size":         1048576, // Maximum request size in bytes
		"message.max.bytes":        1048576, // Maximum message size in bytes
		"queue.buffering.max.ms":   100,     // Maximum time to buffer data (ms)
		"message.send.max.retries": 10,      // Number of message send retries
		"retry.backoff.ms":         100,     // Backoff time between retries (ms)
		"enable.idempotence":       true,    // Enable idempotent producer mode
		"delivery.timeout.ms":      30000,  // Maximum time to wait for message delivery (ms)
		"max.in.flight.requests.per.connection": 1, // Maximum number of unacknowledged requests per connection
		"request.timeout.ms":       5000,   // Maximum time to wait for a request to be completed (ms)
		"reconnect.backoff.max.ms": 1000,   // Maximum time to back off during reconnect attempts (ms)
		"security.protocol":         "plaintext", // Security protocol ("plaintext", "ssl", "sasl_plaintext", etc.)
		"sasl.mechanisms":           "PLAIN",     // SASL mechanism (if using authentication)
		"sasl.username":             "your-username", // SASL username (if using authentication)
		"sasl.password":             "your-password", // SASL password (if using authentication)
		// Add more configurations as needed
	}

	producer, err := kafka.NewProducer(producerConfig)
	if err != nil {
		return nil, err
	}

	return producer, nil
}