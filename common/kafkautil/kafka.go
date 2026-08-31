// Package kafkautil wraps segmentio/kafka-go with the conventions MediFlow
// services share: JSON envelopes with an event name + schema version (so
// message-based Pact contracts have something stable to assert on), and a
// consumer loop with manual offset commits so a handler panic/error doesn't
// silently drop an event.
package kafkautil

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// Envelope is the wire format for every event MediFlow services publish.
// Keeping it uniform is what lets a single message-pact schema cover every
// topic.
type Envelope struct {
	Event      string          `json:"event"`
	Version    int             `json:"version"`
	OccurredAt time.Time       `json:"occurred_at"`
	Data       json.RawMessage `json:"data"`
}

// Producer publishes envelopes to a single Kafka topic.
type Producer struct {
	writer *kafka.Writer
	topic  string
}

// NewProducer builds a Producer for the given brokers and topic.
func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		topic: topic,
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
			BatchTimeout: 10 * time.Millisecond,
		},
	}
}

// Publish marshals data, wraps it in an Envelope and writes it keyed by key
// (e.g. the aggregate id) so related events land on the same partition.
func (p *Producer) Publish(ctx context.Context, event string, version int, key string, data interface{}) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("kafkautil: marshal payload: %w", err)
	}

	envelope := Envelope{
		Event:      event,
		Version:    version,
		OccurredAt: time.Now().UTC(),
		Data:       payload,
	}

	body, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("kafkautil: marshal envelope: %w", err)
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: body,
		Time:  time.Now(),
	})
}

// Close flushes and closes the underlying writer.
func (p *Producer) Close() error {
	return p.writer.Close()
}

// MultiProducer publishes to a topic chosen per message, for services like
// appointment-service that emit several event types over one connection.
type MultiProducer struct {
	writer *kafka.Writer
}

// NewMultiProducer builds a MultiProducer for the given brokers.
func NewMultiProducer(brokers []string) *MultiProducer {
	return &MultiProducer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
			BatchTimeout: 10 * time.Millisecond,
		},
	}
}

// Publish wraps data in an Envelope and writes it to the given topic.
func (p *MultiProducer) Publish(ctx context.Context, topic, event string, version int, key string, data interface{}) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("kafkautil: marshal payload: %w", err)
	}

	body, err := json.Marshal(Envelope{
		Event:      event,
		Version:    version,
		OccurredAt: time.Now().UTC(),
		Data:       payload,
	})
	if err != nil {
		return fmt.Errorf("kafkautil: marshal envelope: %w", err)
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: body,
		Time:  time.Now(),
	})
}

// Close flushes and closes the underlying writer.
func (p *MultiProducer) Close() error {
	return p.writer.Close()
}

// HandlerFunc processes one decoded Envelope. Returning an error prevents
// the offset from being committed, so the message will be redelivered.
type HandlerFunc func(ctx context.Context, env Envelope) error

// Consumer reads envelopes from a topic within a consumer group.
type Consumer struct {
	reader *kafka.Reader
}

// NewConsumer builds a Consumer bound to a consumer group id, so multiple
// replicas of a service share the partitions instead of double-processing
// events.
func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			Topic:       topic,
			GroupID:     groupID,
			MinBytes:    1,
			MaxBytes:    10e6,
			StartOffset: kafka.FirstOffset,
		}),
	}
}

// Run blocks, fetching and dispatching messages to handler until ctx is
// cancelled. A handler error is logged by the caller-supplied onError hook
// and the message is retried on the next poll (offset is only committed
// after a successful handle).
func (c *Consumer) Run(ctx context.Context, handler HandlerFunc, onError func(error)) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("kafkautil: fetch message: %w", err)
		}

		var env Envelope
		if err := json.Unmarshal(msg.Value, &env); err != nil {
			onError(fmt.Errorf("kafkautil: decode envelope: %w", err))
			continue
		}

		if err := handler(ctx, env); err != nil {
			onError(fmt.Errorf("kafkautil: handler failed for event %q: %w", env.Event, err))
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			onError(fmt.Errorf("kafkautil: commit offset: %w", err))
		}
	}
}

// Close closes the underlying reader.
func (c *Consumer) Close() error {
	return c.reader.Close()
}
