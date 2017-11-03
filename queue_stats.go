package workers

import (
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
	queueStats = &QueueStats{
		Queues: make([]*QueueDepth, len(w.managers)),
	}

	config := w.config
	conn := config.Pool.Get()
	defer conn.Close()

	conn.Send("zcard", config.NamespacedKey(w.config.retryQueue))
	i := 0
	for queue, manager := range w.managers {
		conn.Send("llen", config.NamespacedKey("queue", queue))
		conn.Send("llen", config.NamespacedKey(manager.fetch.InprogressQueue()))
		i++
	}
	conn.Flush()

	retryDepth, err := redis.Int(conn.Receive())
	if err != nil {
		return
	}
	queueStats.RetryDepth = retryDepth

	i = 0
	for queue, _ := range w.managers {
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
