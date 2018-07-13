package workers

import (
	"context"
	"errors"
	"testing"

	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"

	"time"
)

var testMiddlewareCalled bool
var failMiddlewareCalled bool

type testMiddleware struct{}

func (l *testMiddleware) Call(queue string, message *Msg, next func() error) (result error) {
	testMiddlewareCalled = true
	return next()
}

type failMiddleware struct{}

func (l *failMiddleware) Call(queue string, message *Msg, next func() error) (result error) {
	failMiddlewareCalled = true
	next()
	return errors.New("failed")
}

func confirm(manager *manager) (msg *Msg) {
	time.Sleep(10 * time.Millisecond)

	select {
	case msg = <-manager.confirm:
	default:
	}

	return
}

func WorkerSpec(c gospec.Context) {
	var processed = make(chan *Args)

	var testJob = (func(message *Msg) error {
		processed <- message.Args()
		return nil
	})

	config := mkDefaultConfig()
	defaultMiddlewares := newDefaultMiddlewares(config)

	manager := newManager(config, "myqueue", testJob, 1)

	c.Specify("newWorker", func() {
		c.Specify("it returns an instance of worker with connection to manager", func() {
			worker := newWorker(manager)
			c.Expect(worker.manager, Equals, manager)
		})
	})

	c.Specify("work", func() {
		worker := newWorker(manager)
		messages := make(chan *Msg)
		message, _ := NewMsg("{\"jid\":\"2309823\",\"args\":[\"foo\",\"bar\"]}")

		c.Specify("calls job with message args", func() {
			ctx := context.Background()
			go worker.work(ctx, messages)
			messages <- message

			args, _ := (<-processed).Array()
			<-manager.confirm

			c.Expect(len(args), Equals, 2)
			c.Expect(args[0], Equals, "foo")
			c.Expect(args[1], Equals, "bar")

			worker.quit(ctx)
		})

		c.Specify("confirms job completed", func() {
			ctx := context.Background()
			go worker.work(ctx, messages)
			messages <- message

			<-processed

			c.Expect(confirm(manager), Equals, message)

			worker.quit(ctx)
		})

		c.Specify("runs defined middleware and confirms", func() {
			worker.manager.mids.Append(&testMiddleware{})
			ctx := context.Background()

			go worker.work(ctx, messages)
			messages <- message

			<-processed
			c.Expect(confirm(manager), Equals, message)
			c.Expect(testMiddlewareCalled, IsTrue)

			worker.quit(ctx)

			worker.manager.mids = defaultMiddlewares
		})

		c.Specify("doesn't confirm if middleware cancels acknowledgement", func() {
			worker.manager.mids.Append(&failMiddleware{})
			ctx := context.Background()

			go worker.work(ctx, messages)
			messages <- message

			<-processed
			c.Expect(confirm(manager), IsNil)
			c.Expect(failMiddlewareCalled, IsTrue)

			worker.quit(ctx)

			worker.manager.mids = defaultMiddlewares
		})

		c.Specify("recovers and cancels if job panics", func() {
			var panicJob = (func(message *Msg) error {
				panic("AHHHH")
				return nil
			})

			manager := newManager(config, "myqueue", panicJob, 1)
			worker := newWorker(manager)

			ctx := context.Background()

			go worker.work(ctx, messages)
			messages <- message

			c.Expect(confirm(manager), Equals, message)

			worker.quit(ctx)
		})
	})
}

func BenchmarkWorkMockedRedis(b *testing.B) {
	acknowledged := make(chan string)
	queue := make(chan interface{})
	ctx := context.Background()

	const msgString = `{"jid":"x","queue":"namespacy:queue:name:ok","args":{"foops":["hello"]},"enqueued_at":"100123917231"}`

	conn := &FuncMockRedisConn{
		DoFn: func(commandName string, args ...interface{}) (reply interface{}, err error) {
			switch commandName {
			case "brpoplpush":
				return <-queue, nil
			case "lrem":
				acknowledged <- args[2].(string)
			case "zrangebyscore":
				return []interface{}{}, nil
			case "lrange":
				return []interface{}{}, nil
			case "exec":
			case "flushdb":
			case "":
			default:
				b.Log("unknown cmd", commandName) //, string(debug.Stack()))
			}
			return nil, nil
		},
	}

	config := mkMockConfig(conn)
	w := mkWorkers(config)

	job := func(message *Msg) error {
		return nil
	}

	w.Process("q", job, 1)
	w.Start(ctx)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			queue <- msgString
			<-acknowledged
		}
	})
}
