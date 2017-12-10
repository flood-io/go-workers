package workers

import (
	"fmt"
	"sync"

	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
)

type customMid struct {
	trace []string
	Base  string
	mutex sync.Mutex
}

func (m *customMid) Call(queue string, message *Msg, next func() error) (err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.trace = append(m.trace, m.Base+"1")
	err = next()
	m.trace = append(m.trace, m.Base+"2")
	return
}

func (m *customMid) Trace() []string {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	t := make([]string, len(m.trace))
	copy(t, m.trace)

	return t
}

func ManagerSpec(c gospec.Context) {
	processed := make(chan *Args)

	testJob := (func(message *Msg) error {
		processed <- message.Args()
		return nil
	})

	config := mkDefaultConfig()

	globalMiddlewares := config.GlobalMiddlewares

	c.Specify("newManager", func() {
		c.Specify("sets queue with namespace", func() {
			manager := newManager(config, "myqueue", testJob, 10)
			c.Expect(manager.queue, Equals, "prod:queue:myqueue")
		})

		c.Specify("sets job function", func() {
			manager := newManager(config, "myqueue", testJob, 10)
			c.Expect(fmt.Sprint(manager.job), Equals, fmt.Sprint(testJob))
		})

		c.Specify("sets worker concurrency", func() {
			manager := newManager(config, "myqueue", testJob, 10)
			c.Expect(manager.concurrency, Equals, 10)
		})

		c.Specify("no per-manager middleware means 'use global Middleware object'", func() {
			manager := newManager(config, "myqueue", testJob, 10)
			c.Expect(manager.mids.Equals(globalMiddlewares), IsTrue)
		})

		c.Specify("per-manager middlewares create separate middleware chains", func() {
			mid1 := customMid{Base: "0"}
			manager := newManager(config, "myqueue", testJob, 10, &mid1)
			c.Expect(manager.mids, Not(Equals), globalMiddlewares)
			c.Expect(len(manager.mids.actions), Equals, len(globalMiddlewares.actions)+1)
		})

	})

	c.Specify("manage", func() {

		conn := config.Pool.Get()
		defer conn.Close()

		message, _ := NewMsg("{\"foo\":\"bar\",\"args\":[\"foo\",\"bar\"]}")
		message2, _ := NewMsg("{\"foo\":\"bar2\",\"args\":[\"foo\",\"bar2\"]}")

		c.Specify("coordinates processing of queue messages", func() {
			manager := newManager(config, "manager1", testJob, 10)

			conn.Do("lpush", "prod:queue:manager1", message.ToJson())
			conn.Do("lpush", "prod:queue:manager1", message2.ToJson())

			manager.start()

			c.Expect(<-processed, Equals, message.Args())
			c.Expect(<-processed, Equals, message2.Args())

			manager.quit()

			len, _ := redis.Int(conn.Do("llen", "prod:queue:manager1"))
			c.Expect(len, Equals, 0)
		})

		c.Specify("per-manager middlwares are called separately, global middleware is called in each manager", func() {
			config := mkDefaultConfig()

			conn := config.Pool.Get()
			defer conn.Close()

			mid1 := customMid{Base: "1"}
			mid2 := customMid{Base: "2"}
			mid3 := customMid{Base: "3"}

			config.GlobalMiddlewares = NewMiddleware(&mid1)

			// XXX fix?
			// oldMiddleware := defaultMiddlewares
			// Middleware = NewMiddleware()
			// Middleware.Append(&mid1)

			manager1 := newManager(config, "manager1", testJob, 10)
			manager2 := newManager(config, "manager2", testJob, 10, &mid2)
			manager3 := newManager(config, "manager3", testJob, 10, &mid3)

			conn.Do("lpush", "prod:queue:manager1", message.ToJson())
			conn.Do("lpush", "prod:queue:manager2", message.ToJson())
			conn.Do("lpush", "prod:queue:manager3", message.ToJson())

			manager1.start()
			manager2.start()
			manager3.start()

			<-processed
			<-processed
			<-processed

			// Middleware = oldMiddleware

			c.Expect(
				arrayCompare(mid1.Trace(), []string{"11", "12", "11", "12", "11", "12"}),
				IsTrue,
			)
			c.Expect(
				arrayCompare(mid2.Trace(), []string{"21", "22"}),
				IsTrue,
			)
			c.Expect(
				arrayCompare(mid3.Trace(), []string{"31", "32"}),
				IsTrue,
			)

			manager1.quit()
			manager2.quit()
			manager3.quit()
		})

		c.Specify("prepare stops fetching new messages from queue", func() {
			manager := newManager(config, "manager2", testJob, 10)
			manager.start()

			manager.prepareForQuit()

			conn.Do("lpush", "prod:queue:manager2", message)
			conn.Do("lpush", "prod:queue:manager2", message2)

			manager.quit()

			len, _ := redis.Int(conn.Do("llen", "prod:queue:manager2"))
			c.Expect(len, Equals, 2)
		})
	})
}
