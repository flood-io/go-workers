package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
)

func ScheduledSpec(c gospec.Context) {
	config := mkDefaultConfig()
	config.SetNamespace("prod:")

	scheduled := newScheduled(config, config.retryQueue)

	c.Specify("empties retry queues up to the current time", func() {
		conn := config.Pool.Get()
		defer conn.Close()

		now := nowToSecondsWithNanoPrecision()

		message1, _ := NewMsg("{\"queue\":\"default\",\"foo\":\"bar1\"}")
		message2, _ := NewMsg("{\"queue\":\"myqueue\",\"foo\":\"bar2\"}")
		message3, _ := NewMsg("{\"queue\":\"default\",\"foo\":\"bar3\"}")

		conn.Do("zadd", "prod:"+config.retryQueue, now-60.0, message1.ToJson())
		conn.Do("zadd", "prod:"+config.retryQueue, now-10.0, message2.ToJson())
		conn.Do("zadd", "prod:"+config.retryQueue, now+60.0, message3.ToJson())

		scheduled.poll()

		defaultCount, _ := redis.Int(conn.Do("llen", "prod:queue:default"))
		myqueueCount, _ := redis.Int(conn.Do("llen", "prod:queue:myqueue"))
		pending, _ := redis.Int(conn.Do("zcard", "prod:"+config.retryQueue))

		c.Expect(defaultCount, Equals, 1)
		c.Expect(myqueueCount, Equals, 1)
		c.Expect(pending, Equals, 1)
	})
}
