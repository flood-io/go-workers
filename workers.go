package workers

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
)

const (
	RETRY_KEY          = "goretry"
	SCHEDULED_JOBS_KEY = "schedule"
)

var Logger WorkersLogger = log.New(os.Stdout, "workers: ", log.Ldate|log.Lmicroseconds)

var managers = make(map[string]*manager)
var schedule *scheduled
var control = make(map[string]chan string)
var access sync.Mutex
var started bool

func newDefaultMiddlewares(config *config) *Middlewares {
	return NewMiddleware(
		&MiddlewareLogging{},
		&MiddlewareRetry{config},
		&MiddlewareStats{config},
	)
}

func Process(config *config, queue string, job jobFunc, concurrency int, mids ...Action) {
	access.Lock()
	defer access.Unlock()

	managers[queue] = newManager(config, queue, job, concurrency, mids...)
}

func Run(config *config) {
	Start(config)
	go handleSignals()
	waitForExit()
}

func ResetManagers() error {
	access.Lock()
	defer access.Unlock()

	if started {
		return errors.New("Cannot reset worker managers while workers are running")
	}

	managers = make(map[string]*manager)

	return nil
}

func Start(config *config) {
	access.Lock()
	defer access.Unlock()

	if started {
		return
	}

	runHooks(beforeStart)
	startSchedule(config)
	startManagers()

	started = true
}

func Quit() {
	access.Lock()
	defer access.Unlock()

	if !started {
		return
	}

	quitManagers()
	quitSchedule()
	runHooks(duringDrain)
	waitForExit()

	started = false
}

func StatsServer(config *config, port int) {
	statsClosure := func(w http.ResponseWriter, req *http.Request) {
		Stats(config, w, req)
	}
	http.HandleFunc("/stats", statsClosure)

	Logger.Println("Stats are available at", fmt.Sprint("http://localhost:", port, "/stats"))

	if err := http.ListenAndServe(fmt.Sprint(":", port), nil); err != nil {
		Logger.Println(err)
	}
}

func startSchedule(config *config) {
	if schedule == nil {
		schedule = newScheduled(config, RETRY_KEY, SCHEDULED_JOBS_KEY)
	}

	schedule.start()
}

func quitSchedule() {
	if schedule != nil {
		schedule.quit()
		schedule = nil
	}
}

func startManagers() {
	for _, manager := range managers {
		manager.start()
	}
}

func quitManagers() {
	for _, m := range managers {
		go (func(m *manager) { m.quit() })(m)
	}
}

func waitForExit() {
	for _, manager := range managers {
		manager.Wait()
	}
}
