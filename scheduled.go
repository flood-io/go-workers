package workers

import (
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

type scheduled struct {
	config *config
	keys   []string
	closed chan bool
	exit   chan bool
}

func (s *scheduled) start() {
	go (func() {
		for {
			select {
			case <-s.closed:
				return
			default:
			}

			s.poll()

			time.Sleep(time.Duration(s.config.PollInterval) * time.Second)
		}
	})()
}

func (s *scheduled) quit() {
	close(s.closed)
}

func (s *scheduled) poll() {
	conn := s.config.Pool.Get()

	now := nowToSecondsWithNanoPrecision()

	for _, key := range s.keys {
		key = s.config.Namespace + key
		for {
			messages, _ := redis.Strings(conn.Do("zrangebyscore", key, "-inf", now, "limit", 0, 1))

			if len(messages) == 0 {
				break
			}

			message, _ := NewMsg(messages[0])

			if removed, _ := redis.Bool(conn.Do("zrem", key, messages[0])); removed {
				queue, _ := message.Get("queue").String()
				queue = strings.TrimPrefix(queue, s.config.Namespace)
				message.Set("enqueued_at", nowToSecondsWithNanoPrecision())
				conn.Do("lpush", s.config.Namespace+"queue:"+queue, message.ToJson())
			}
		}
	}

	conn.Close()
}

func newScheduled(config *config, keys ...string) *scheduled {
	return &scheduled{config, keys, make(chan bool), make(chan bool)}
}
