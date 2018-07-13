package workers

import (
	"context"
	"strings"
	"sync"
)

type manager struct {
	config      *config
	queue       string
	fetch       Fetcher
	job         jobFunc
	concurrency int
	workers     []*worker
	workersM    *sync.Mutex
	confirm     chan *Msg
	stop        chan bool
	exit        chan bool
	mids        *Middlewares
	*sync.WaitGroup
}

func (m *manager) start(ctx context.Context) {
	m.Add(1)
	m.loadWorkers(ctx)
	go m.manage(ctx)
}

func (m *manager) prepareForQuit(ctx context.Context) {
	if !m.fetch.Closed() {
		m.fetch.Close()
	}
}

func (m *manager) quit(ctx context.Context) {
	Logger.Println("quitting queue", m.queueName(), "(waiting for", m.processing(), "/", len(m.workers), "workers).")
	m.prepareForQuit(ctx)

	m.workersM.Lock()
	for _, worker := range m.workers {
		worker.quit(ctx)
	}
	m.workersM.Unlock()

	m.stop <- true
	<-m.exit

	m.reset()

	m.Done()
}

func (m *manager) manage(ctx context.Context) {
	Logger.Println("processing queue", m.queueName(), "with", m.concurrency, "workers.")

	go m.fetch.Fetch(ctx)

	for {
		select {
		case message := <-m.confirm:
			m.fetch.Acknowledge(ctx, message)
		case <-m.stop:
			m.exit <- true
			break
		}
	}
}

func (m *manager) loadWorkers(ctx context.Context) {
	m.workersM.Lock()
	for i := 0; i < m.concurrency; i++ {
		m.workers[i] = newWorker(m)
		m.workers[i].start(ctx)
	}
	m.workersM.Unlock()
}

func (m *manager) processing() (count int) {
	m.workersM.Lock()
	for _, worker := range m.workers {
		if worker.isProcessing() {
			count++
		}
	}
	m.workersM.Unlock()
	return
}

func (m *manager) queueName() string {
	return strings.Replace(m.queue, "queue:", "", 1)
}

func (m *manager) reset() {
	m.fetch = m.config.Fetch(m.queue)
}

func newManager(config *config, queue string, job jobFunc, concurrency int, mids ...Action) *manager {
	m := &manager{
		config,
		config.NamespacedKey("queue", queue),
		nil,
		job,
		concurrency,
		make([]*worker, concurrency),
		&sync.Mutex{},
		make(chan *Msg),
		make(chan bool),
		make(chan bool),
		config.GlobalMiddlewares.AppendToCopy(mids),
		&sync.WaitGroup{},
	}

	m.reset()

	return m
}
