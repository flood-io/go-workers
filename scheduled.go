package workers

import (
	"context"
	"time"

	"github.com/garyburd/redigo/redis"
)

type scheduled struct {
	config *config
	keys   []string
	closed chan bool
	exit   chan bool
}

func (s *scheduled) start(ctx context.Context) {
	go (func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.closed:
				return
			case <-time.After(time.Duration(s.config.PollInterval) * time.Second):
				s.poll(ctx)
			}
		}
	})()
}

func (s *scheduled) quit(ctx context.Context) {
	close(s.closed)
}

func (s *scheduled) poll(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	}
	conn := s.config.Pool.Get()
	defer conn.Close()

	now := nowToSecondsWithNanoPrecision()

	for _, key := range s.keys {
		key = s.config.NamespacedKey(key)
		for {
			messages, _ := redis.Strings(conn.Do("zrangebyscore", key, "-inf", now, "limit", 0, 1))

			if len(messages) == 0 {
				break
			}

			message, _ := NewMsg(messages[0])

			if removed, _ := redis.Bool(conn.Do("zrem", key, messages[0])); removed {
				queue, _ := message.Get("queue").String()
				queue = s.config.TrimKeyNamespace(queue)
				message.Set("enqueued_at", nowToSecondsWithNanoPrecision())
				conn.Do("lpush", s.config.NamespacedKey("queue", queue), message.ToJson())
			}
		}
	}
}

func newScheduled(config *config, keys ...string) *scheduled {
	return &scheduled{config, keys, make(chan bool), make(chan bool)}
}
