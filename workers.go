package workers

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
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

func NewWorkers(config *config) *Workers {
	return &Workers{
		config,
		make(map[string]*manager),
		nil,
		make(map[string]chan string),
		sync.Mutex{},
		false,
		nil,
		nil,
	}
}

func newDefaultMiddlewares(config *config) *Middlewares {
	return NewMiddleware(
		&MiddlewareLogging{},
		&MiddlewareRetry{config},
		&MiddlewareStats{config},
	)
}

func (w *Workers) Process(queue string, job jobFunc, concurrency int, mids ...Action) {
	w.access.Lock()
	defer w.access.Unlock()

	w.managers[queue] = newManager(w.config, queue, job, concurrency, mids...)
}

func (w *Workers) Run() {
	w.Start()
	go w.handleSignals()
	w.WaitForExit()
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

func (w *Workers) Start() {
	w.access.Lock()
	defer w.access.Unlock()

	if w.started {
		return
	}

	runHooks(w.beforeStart)
	w.startSchedule()
	w.startManagers()

	w.started = true
}

func (w *Workers) Quit() {
	w.access.Lock()
	defer w.access.Unlock()

	if !w.started {
		return
	}

	w.quitManagers()
	w.quitSchedule()
	runHooks(w.duringDrain)
	w.WaitForExit()

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

func (w *Workers) startSchedule() {
	if w.schedule == nil {
		w.schedule = newScheduled(w.config, w.config.retryQueue, w.config.scheduledJobsQueue)
	}

	w.schedule.start()
}

func (w *Workers) quitSchedule() {
	if w.schedule != nil {
		w.schedule.quit()
		w.schedule = nil
	}
}

func (w *Workers) startManagers() {
	for _, manager := range w.managers {
		manager.start()
	}
}

func (w *Workers) quitManagers() {
	for _, m := range w.managers {
		go (func(m *manager) { m.quit() })(m)
	}
}

func (w *Workers) WaitForExit() {
	for _, manager := range w.managers {
		manager.Wait()
	}
}
