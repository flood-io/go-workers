package workers

import (
	"context"
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
)

type Fetcher interface {
	Queue() string
	Fetch(context.Context)
	Acknowledge(context.Context, *Msg)
	Ready() chan bool
	FinishedWork() chan bool
	Messages() chan *Msg
	Close()
	Closed() bool
	InprogressQueue() string
}

type fetch struct {
	config          *config
	queue           string
	ready           chan bool
	finishedwork    chan bool
	messages        chan *Msg
	stop            chan bool
	exit            chan bool
	closed          chan bool
	inprogressQueue string
}

func NewFetch(config *config, queue string, messages chan *Msg, ready chan bool) Fetcher {
	return &fetch{
		config,
		queue,
		ready,
		make(chan bool),
		messages,
		make(chan bool),
		make(chan bool),
		make(chan bool),
		fmt.Sprint(queue, ":", config.processId, ":inprogress"),
	}
}

func (f *fetch) Queue() string {
	return f.queue
}

func (f *fetch) processOldMessages(ctx context.Context) {
	messages := f.inprogressMessages(ctx)

	for _, message := range messages {
		<-f.Ready()
		f.sendMessage(ctx, message)
	}
}

func (f *fetch) Fetch(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	f.processOldMessages(ctx)

	go func() {
		for {
			// f.Close() has been called
			if f.Closed() {
				break
			}
			<-f.Ready()
			f.tryFetchMessage(ctx)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			// Stop the redis-polling goroutine
			close(f.closed)
			// Signal to Close() that the fetcher has stopped
			close(f.exit)
			break
		case <-f.stop:
			cancel()
		}
	}
}

func (f *fetch) tryFetchMessage(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	conn := f.config.Pool.Get()
	defer conn.Close()

	message, err := redis.String(conn.Do("brpoplpush", f.queue, f.inprogressQueue, 1))

	if err != nil {
		// If redis returns null, the queue is empty. Just ignore the error.
		if err != redis.ErrNil {
			Logger.Println("ERR: ", err)
			time.Sleep(1 * time.Second)
		}
	} else {
		f.sendMessage(ctx, message)
	}
}

func (f *fetch) sendMessage(ctx context.Context, message string) {
	msg, err := NewMsg(message)

	if err != nil {
		Logger.Println("ERR: Couldn't create message from", message, ":", err)
		return
	}

	f.Messages() <- msg
}

func (f *fetch) Acknowledge(ctx context.Context, message *Msg) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	conn := f.config.Pool.Get()
	defer conn.Close()

	conn.Do("lrem", f.inprogressQueue, -1, message.OriginalJson())
}

func (f *fetch) Messages() chan *Msg {
	return f.messages
}

func (f *fetch) Ready() chan bool {
	return f.ready
}

func (f *fetch) FinishedWork() chan bool {
	return f.finishedwork
}

func (f *fetch) Close() {
	if f.Closed() {
		return
	}
	f.stop <- true
	<-f.exit
}

func (f *fetch) Closed() bool {
	select {
	case <-f.closed:
		return true
	default:
		return false
	}
}

func (f *fetch) inprogressMessages(ctx context.Context) []string {
	select {
	case <-ctx.Done():
		return []string{}
	default:
	}
	conn := f.config.Pool.Get()
	defer conn.Close()

	messages, err := redis.Strings(conn.Do("lrange", f.inprogressQueue, 0, -1))
	if err != nil {
		Logger.Println("ERR: ", err)
	}

	return messages
}

func (f *fetch) InprogressQueue() string {
	return f.inprogressQueue
}
