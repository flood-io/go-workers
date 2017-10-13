package workers

import (
	"time"

	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
)

func MiddlewareStatsSpec(c gospec.Context) {
	var job = (func(message *Msg) {
		// noop
	})

	config := mkDefaultConfig()
	config.SetNamespace("prod:")

	layout := "2006-01-02"
	manager := newManager(config, "myqueue", job, 1)
	worker := newWorker(manager)
	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true}")

	c.Specify("increments processed stats", func() {
		conn := config.Pool.Get()
		defer conn.Close()

		count, _ := redis.Int(conn.Do("get", "prod:stat:processed"))
		dayCount, _ := redis.Int(conn.Do("get", "prod:stat:processed:"+time.Now().UTC().Format(layout)))

		c.Expect(count, Equals, 0)
		c.Expect(dayCount, Equals, 0)

		worker.process(message)

		count, _ = redis.Int(conn.Do("get", "prod:stat:processed"))
		dayCount, _ = redis.Int(conn.Do("get", "prod:stat:processed:"+time.Now().UTC().Format(layout)))

		c.Expect(count, Equals, 1)
		c.Expect(dayCount, Equals, 1)
	})

	c.Specify("failed job", func() {
		var job = (func(message *Msg) {
			panic("AHHHH")
		})

		manager := newManager(config, "myqueue", job, 1)
		worker := newWorker(manager)

		c.Specify("increments failed stats", func() {
			conn := config.Pool.Get()
			defer conn.Close()

			count, _ := redis.Int(conn.Do("get", "prod:stat:failed"))
			dayCount, _ := redis.Int(conn.Do("get", "prod:stat:failed:"+time.Now().UTC().Format(layout)))

			c.Expect(count, Equals, 0)
			c.Expect(dayCount, Equals, 0)

			worker.process(message)

			count, _ = redis.Int(conn.Do("get", "prod:stat:failed"))
			dayCount, _ = redis.Int(conn.Do("get", "prod:stat:failed:"+time.Now().UTC().Format(layout)))

			c.Expect(count, Equals, 1)
			c.Expect(dayCount, Equals, 1)
		})
	})
}
