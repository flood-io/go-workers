package workers

import (
	"time"
)

type GoWorkers interface {
	Run()
	Start()
	Quit()
	WaitForExit()
	ResetManagers() error

	Process(queue string, job jobFunc, concurrency int, mids ...Action)

	Enqueue(queue, class string, args interface{}) (string, error)
	EnqueueIn(queue, class string, in float64, args interface{}) (string, error)
	EnqueueAt(queue, class string, at time.Time, args interface{}) (string, error)
	EnqueueWithOptions(queue, class string, args interface{}, opts EnqueueOptions) (string, error)

	BeforeStart(f func())
	DuringDrain(f func())

	Ping() error
	QueueStats() (queueStats *QueueStats, err error)

	Namespace() string
	NamespacedKey(keys ...string) string
	TrimKeyNamespace(key string) string
}
