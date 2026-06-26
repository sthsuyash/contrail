package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

// Stdout writes newline-delimited JSON.
//
// Its purpose is the first five minutes after a clone: paired with replay mode
// it lets the ingester run and produce visible output with no broker, no
// Docker, and no credentials. It is also what makes the pipeline pipeable into
// jq while debugging a decoding problem.
type Stdout struct {
	mu  sync.Mutex
	w   *bufio.Writer
	enc *json.Encoder
}

// NewStdout writes NDJSON to w.
func NewStdout(w io.Writer) *Stdout {
	buffered := bufio.NewWriterSize(w, 1<<16)
	return &Stdout{w: buffered, enc: json.NewEncoder(buffered)}
}

// Describe implements Sink.
func (s *Stdout) Describe() string { return "stdout (newline-delimited JSON)" }

// Write implements Sink.
func (s *Stdout) Write(ctx context.Context, records []Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range records {
		if err := s.enc.Encode(&records[i]); err != nil {
			return fmt.Errorf("encoding record %d: %w", i, err)
		}
	}
	// Flushing per batch rather than per record keeps a poll's worth of output
	// contiguous when several writers share the stream, without paying a
	// syscall for every one of several hundred aircraft.
	return s.w.Flush()
}

// Close implements Sink.
func (s *Stdout) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Flush()
}
