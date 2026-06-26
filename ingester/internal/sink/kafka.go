package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Kafka produces records to a Kafka-compatible broker (Redpanda in this stack).
type Kafka struct {
	client  *kgo.Client
	topic   string
	brokers []string

	mu     sync.Mutex
	closed bool
}

// NewKafka connects a producer to the given brokers.
func NewKafka(brokers []string, topic string) (*Kafka, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka sink needs at least one broker")
	}
	if topic == "" {
		return nil, fmt.Errorf("kafka sink needs a topic")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		// Idempotent production is on by default in franz-go and is left on
		// deliberately: a broker-side retry must not turn one observation into
		// two. Deduplication downstream would catch it, but a duplicate that
		// never enters the log is cheaper than one filtered out of every
		// subsequent read.
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		// State vectors are small and highly repetitive across a poll (the
		// same country strings and callsign prefixes recur hundreds of times),
		// so compression earns its CPU several times over.
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	)
	if err != nil {
		return nil, fmt.Errorf("creating kafka client: %w", err)
	}
	return &Kafka{client: client, topic: topic, brokers: brokers}, nil
}

// Describe implements Sink.
func (k *Kafka) Describe() string {
	return fmt.Sprintf("kafka topic %q via %s", k.topic, strings.Join(k.brokers, ","))
}

// Write implements Sink, producing the batch and waiting for acknowledgement.
//
// Records are keyed by ICAO24 so every observation of one aircraft lands on the
// same partition and therefore stays in order relative to its own history.
// Sessionization reconstructs flights by walking an aircraft's positions in
// sequence, so per-aircraft ordering is the one guarantee it cannot do without.
// Global ordering across aircraft is neither needed nor affordable.
func (k *Kafka) Write(ctx context.Context, records []Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	k.mu.Lock()
	if k.closed {
		k.mu.Unlock()
		return fmt.Errorf("kafka sink is closed")
	}
	k.mu.Unlock()

	msgs := make([]*kgo.Record, 0, len(records))
	for i := range records {
		payload, err := json.Marshal(&records[i])
		if err != nil {
			return fmt.Errorf("encoding record %d: %w", i, err)
		}
		msgs = append(msgs, &kgo.Record{
			Key:   []byte(records[i].ICAO24),
			Value: payload,
		})
	}

	// ProduceSync blocks until every record is acknowledged. The ingester polls
	// on an interval measured in tens of seconds, so there is no throughput
	// reason to fire-and-forget, and a synchronous write means a failure is
	// reported to the poll loop that caused it rather than surfacing later in
	// an async callback with no context.
	if err := k.client.ProduceSync(ctx, msgs...).FirstErr(); err != nil {
		return fmt.Errorf("producing %d records to %q: %w", len(msgs), k.topic, err)
	}
	return nil
}

// Close implements Sink.
func (k *Kafka) Close() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.closed {
		return nil
	}
	k.closed = true
	k.client.Close()
	return nil
}
