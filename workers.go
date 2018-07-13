package workers

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/garyburd/redigo/redis"
)

var Logger WorkersLogger = log.New(os.Stdout, "workers: ", log.Ldate|log.Lmicroseconds)

type Workers struct {
	config      *config
	managers    map[string]*manager
	schedule    *scheduled
	control     map[string]chan string
	access      sync.Mutex
	started     bool
	beforeStart []func()
	duringDrain []func()
}

// ensure that Workers struct fulfils GoWorkers interface
var _ GoWorkers = (*Workers)(nil)

func NewWorkers(config *config) *Workers {
	return &Workers{
		config:   config,
		managers: make(map[string]*manager),
		control:  make(map[string]chan string),
		access:   sync.Mutex{},
		started:  false,
	}
}

func newDefaultMiddlewares(config *config) *Middlewares {
	return NewMiddleware(
		&MiddlewareRetry{config},
		&MiddlewareStats{config},
	)
}

func (w *Workers) RedisPool() *redis.Pool {
	return w.config.Pool
}

func (w *Workers) Process(queue string, job jobFunc, concurrency int, mids ...Action) {
	w.access.Lock()
	defer w.access.Unlock()

	w.managers[queue] = newManager(w.config, queue, job, concurrency, mids...)
}

func (w *Workers) Run(ctx context.Context) error {
	w.Start(ctx)
	go w.handleSignals()
	w.WaitForExit(ctx)
	return nil
}

func (w *Workers) ResetManagers() error {
	w.access.Lock()
	defer w.access.Unlock()

	if w.started {
		return errors.New("Cannot reset worker managers while workers are running")
	}

	w.managers = make(map[string]*manager)

	return nil
}

func (w *Workers) Start(ctx context.Context) {
	w.access.Lock()
	defer w.access.Unlock()

	if w.started {
		return
	}

	runHooks(w.beforeStart)
	w.startSchedule(ctx)
	w.startManagers(ctx)

	w.started = true
}

func (w *Workers) Quit(ctx context.Context) {
	w.access.Lock()
	defer w.access.Unlock()

	if !w.started {
		return
	}

	w.quitManagers(ctx)
	w.quitSchedule(ctx)
	runHooks(w.duringDrain)
	w.WaitForExit(ctx)

	w.started = false
}

func StatsServer(workers *Workers, port int) {
	statsClosure := func(w http.ResponseWriter, req *http.Request) {
		Stats(workers, w, req)
	}
	http.HandleFunc("/stats", statsClosure)

	Logger.Println("Stats are available at", fmt.Sprint("http://localhost:", port, "/stats"))

	if err := http.ListenAndServe(fmt.Sprint(":", port), nil); err != nil {
		Logger.Println(err)
	}
}

func (w *Workers) startSchedule(ctx context.Context) {
	if w.schedule == nil {
		w.schedule = newScheduled(w.config, w.config.retryQueue, w.config.scheduledJobsQueue)
	}

	w.schedule.start(ctx)
}

func (w *Workers) quitSchedule(ctx context.Context) {
	if w.schedule != nil {
		w.schedule.quit(ctx)
		w.schedule = nil
	}
}

func (w *Workers) startManagers(ctx context.Context) {
	for _, manager := range w.managers {
		manager.start(ctx)
	}
}

func (w *Workers) quitManagers(ctx context.Context) {
	for _, m := range w.managers {
		go (func(m *manager) { m.quit(ctx) })(m)
	}
}

func (w *Workers) WaitForExit(ctx context.Context) {
	for _, manager := range w.managers {
		manager.Wait()
	}
}
