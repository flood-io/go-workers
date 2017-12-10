package workers

import (
	"fmt"

	"github.com/garyburd/redigo/redis"
)

type QueueStats struct {
	Queues     []*QueueDepth
	RetryDepth int
}

type QueueDepth struct {
	Name       string
	InProgress int
	Queued     int
}

func (w *Workers) QueueStats() (queueStats *QueueStats, err error) {
	config := w.config
	conn := config.Pool.Get()
	defer conn.Close()

	queues, err := redis.Strings(conn.Do("SMEMBERS", config.NamespacedKey("queues")))
	if err != nil {
		return
	}

	queueStats = &QueueStats{
		Queues: make([]*QueueDepth, len(queues)),
	}

	conn.Send("zcard", config.NamespacedKey(w.config.retryQueue))
	i := 0
	for _, queue := range queues {
		conn.Send("llen", config.NamespacedKey("queue", queue))
		inprogressQueue := fmt.Sprint(queue, ":", config.processId, ":inprogress")
		conn.Send("llen", config.NamespacedKey(inprogressQueue))
		i++
	}
	conn.Flush()

	retryDepth, err := redis.Int(conn.Receive())
	if err != nil {
		return
	}
	queueStats.RetryDepth = retryDepth

	for i, queue := range queues {
		var queued, inprogress int
		queued, err = redis.Int(conn.Receive())
		if err != nil {
			return
		}
		inprogress, err = redis.Int(conn.Receive())
		if err != nil {
			return
		}

		queueStats.Queues[i] = &QueueDepth{
			queue,
			inprogress,
			queued,
		}
	}

	return
}
