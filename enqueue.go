package workers

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

const (
	NanoSecondPrecision = 1000000000.0
)

type EnqueueData struct {
	Queue      string      `json:"queue,omitempty"`
	Class      string      `json:"class"`
	Args       interface{} `json:"args"`
	Jid        string      `json:"jid"`
	EnqueuedAt float64     `json:"enqueued_at"`
	EnqueueOptions
}

type EnqueueOptions struct {
	RetryCount int     `json:"retry_count,omitempty"`
	Retry      bool    `json:"retry,omitempty"`
	At         float64 `json:"at,omitempty"`
}

func generateJid() string {
	// Return 12 random bytes as 24 character hex
	b := make([]byte, 12)
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", b)
}

func (w *Workers) Enqueue(queue, class string, args interface{}) (string, error) {
	return w.EnqueueWithOptions(queue, class, args, EnqueueOptions{At: nowToSecondsWithNanoPrecision()})
}

func (w *Workers) EnqueueIn(queue, class string, in float64, args interface{}) (string, error) {
	return w.EnqueueWithOptions(queue, class, args, EnqueueOptions{At: nowToSecondsWithNanoPrecision() + in})
}

func (w *Workers) EnqueueAt(queue, class string, at time.Time, args interface{}) (string, error) {
	return w.EnqueueWithOptions(queue, class, args, EnqueueOptions{At: timeToSecondsWithNanoPrecision(at)})
}

func (w *Workers) EnqueueWithOptions(queue, class string, args interface{}, opts EnqueueOptions) (string, error) {
	now := nowToSecondsWithNanoPrecision()
	data := EnqueueData{
		Queue:          queue,
		Class:          class,
		Args:           args,
		Jid:            generateJid(),
		EnqueuedAt:     now,
		EnqueueOptions: opts,
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		return "", err
	}

	if now < opts.At {
		err := w.enqueueAt(data.At, bytes)
		return data.Jid, err
	}

	conn := w.config.Pool.Get()
	defer conn.Close()

	_, err = conn.Do("sadd", w.config.NamespacedKey("queues"), queue)
	if err != nil {
		return "", err
	}
	queue = w.config.NamespacedKey("queue", queue)
	_, err = conn.Do("rpush", queue, bytes)
	if err != nil {
		return "", err
	}

	return data.Jid, nil
}

func (w *Workers) enqueueAt(at float64, bytes []byte) error {
	conn := w.config.Pool.Get()
	defer conn.Close()

	_, err := conn.Do(
		"zadd",
		w.config.NamespacedKey(SCHEDULED_JOBS_KEY), at, bytes,
	)
	if err != nil {
		return err
	}

	return nil
}

func timeToSecondsWithNanoPrecision(t time.Time) float64 {
	return float64(t.UnixNano()) / NanoSecondPrecision
}

func durationToSecondsWithNanoPrecision(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / NanoSecondPrecision
}

func nowToSecondsWithNanoPrecision() float64 {
	return timeToSecondsWithNanoPrecision(time.Now())
}
