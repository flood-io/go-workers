package workers

import (
	"time"
)

type MiddlewareStats struct {
	config *config
}

func (l *MiddlewareStats) Call(queue string, message *Msg, next func() bool) (acknowledge bool) {
	defer func() {
		if e := recover(); e != nil {
			incrementStats(l.config, "failed")
			panic(e)
		}
	}()

	acknowledge = next()

	incrementStats(l.config, "processed")

	return
}

func incrementStats(config *config, metric string) {
	conn := config.Pool.Get()
	defer conn.Close()

	today := time.Now().UTC().Format("2006-01-02")

	conn.Send("multi")
	conn.Send("incr", config.NamespacedKey("stat", metric))
	conn.Send("incr", config.NamespacedKey("stat", metric, today))

	if _, err := conn.Do("exec"); err != nil {
		Logger.Println("couldn't save stats:", err)
	}
}
