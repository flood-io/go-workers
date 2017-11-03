package workers

import (
	"errors"
	"time"

	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
)

func MiddlewareRetrySpec(c gospec.Context) {
	var erroringJob = (func(message *Msg) error {
		return errors.New("AHHHH")
	})

	config := mkDefaultConfig()
	config.SetNamespace("prod")

	var wares = NewMiddleware(
		&MiddlewareRetry{config},
	)

	layout := "2006-01-02 15:04:05 MST"
	manager := newManager(config, "myqueue", erroringJob, 1)
	worker := newWorker(manager)

	c.Specify("puts messages in retry queue when they fail", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true}")

		wares.call("myqueue", message, func() error {
			return worker.process(message)
		})

		conn := config.Pool.Get()
		defer conn.Close()

		retries, _ := redis.Strings(conn.Do("zrange", config.NamespacedKey(config.retryQueue), 0, 1))
		c.Expect(retries[0], Equals, message.ToJson())
	})

	c.Specify("allows disabling retries", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"retry\":false}")

		wares.call("myqueue", message, func() error {
			return worker.process(message)
		})

		conn := config.Pool.Get()
		defer conn.Close()

		count, _ := redis.Int(conn.Do("zcard", config.NamespacedKey(config.retryQueue)))
		c.Expect(count, Equals, 0)
	})

	c.Specify("doesn't retry by default", func() {
		message, _ := NewMsg("{\"jid\":\"2\"}")

		wares.call("myqueue", message, func() error {
			return worker.process(message)
		})

		conn := config.Pool.Get()
		defer conn.Close()

		count, _ := redis.Int(conn.Do("zcard", config.NamespacedKey(config.retryQueue)))
		c.Expect(count, Equals, 0)
	})

	c.Specify("allows numeric retries", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"retry\":5}")

		wares.call("myqueue", message, func() error {
			return worker.process(message)
		})

		conn := config.Pool.Get()
		defer conn.Close()

		retries, _ := redis.Strings(conn.Do("zrange", config.NamespacedKey(config.retryQueue), 0, 1))
		c.Expect(retries[0], Equals, message.ToJson())
	})

	c.Specify("handles new failed message", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true}")

		wares.call("myqueue", message, func() error {
			return worker.process(message)
		})

		conn := config.Pool.Get()
		defer conn.Close()

		retries, _ := redis.Strings(conn.Do("zrange", config.NamespacedKey(config.retryQueue), 0, 1))
		message, _ = NewMsg(retries[0])

		queue, _ := message.Get("queue").String()
		error_message, _ := message.Get("error_message").String()
		error_class, _ := message.Get("error_class").String()
		retry_count, _ := message.Get("retry_count").Int()
		error_backtrace, _ := message.Get("error_backtrace").String()
		failed_at, _ := message.Get("failed_at").String()

		c.Expect(queue, Equals, config.NamespacedKey("myqueue"))
		c.Expect(error_message, Equals, "AHHHH")
		c.Expect(error_class, Equals, "")
		c.Expect(retry_count, Equals, 0)
		c.Expect(error_backtrace, Equals, "")
		c.Expect(failed_at, Equals, time.Now().UTC().Format(layout))
	})

	c.Specify("handles recurring failed message", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true,\"queue\":\"default\",\"error_message\":\"bam\",\"failed_at\":\"2013-07-20 14:03:42 UTC\",\"retry_count\":10}")

		wares.call("myqueue", message, func() error {
			return worker.process(message)
		})

		conn := config.Pool.Get()
		defer conn.Close()

		retries, _ := redis.Strings(conn.Do("zrange", config.NamespacedKey(config.retryQueue), 0, 1))
		message, _ = NewMsg(retries[0])

		queue, _ := message.Get("queue").String()
		error_message, _ := message.Get("error_message").String()
		retry_count, _ := message.Get("retry_count").Int()
		failed_at, _ := message.Get("failed_at").String()
		retried_at, _ := message.Get("retried_at").String()

		c.Expect(queue, Equals, "prod:myqueue")
		c.Expect(error_message, Equals, "AHHHH")
		c.Expect(retry_count, Equals, 11)
		c.Expect(failed_at, Equals, "2013-07-20 14:03:42 UTC")
		c.Expect(retried_at, Equals, time.Now().UTC().Format(layout))
	})

	c.Specify("handles recurring failed message with customized max", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"retry\":10,\"queue\":\"default\",\"error_message\":\"bam\",\"failed_at\":\"2013-07-20 14:03:42 UTC\",\"retry_count\":8}")

		wares.call("myqueue", message, func() error {
			return worker.process(message)
		})

		conn := config.Pool.Get()
		defer conn.Close()

		retries, _ := redis.Strings(conn.Do("zrange", config.NamespacedKey(config.retryQueue), 0, 1))
		message, _ = NewMsg(retries[0])

		queue, _ := message.Get("queue").String()
		error_message, _ := message.Get("error_message").String()
		retry_count, _ := message.Get("retry_count").Int()
		failed_at, _ := message.Get("failed_at").String()
		retried_at, _ := message.Get("retried_at").String()

		c.Expect(queue, Equals, "prod:myqueue")
		c.Expect(error_message, Equals, "AHHHH")
		c.Expect(retry_count, Equals, 9)
		c.Expect(failed_at, Equals, "2013-07-20 14:03:42 UTC")
		c.Expect(retried_at, Equals, time.Now().UTC().Format(layout))
	})

	c.Specify("doesn't retry after default number of retries", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true,\"retry_count\":25}")

		wares.call("myqueue", message, func() error {
			return worker.process(message)
		})

		conn := config.Pool.Get()
		defer conn.Close()

		count, _ := redis.Int(conn.Do("zcard", config.NamespacedKey(config.retryQueue)))
		c.Expect(count, Equals, 0)
	})

	c.Specify("doesn't retry after customized number of retries", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"retry\":3,\"retry_count\":3}")

		wares.call("myqueue", message, func() error {
			return worker.process(message)
		})

		conn := config.Pool.Get()
		defer conn.Close()

		count, _ := redis.Int(conn.Do("zcard", config.NamespacedKey(config.retryQueue)))
		c.Expect(count, Equals, 0)
	})
}
