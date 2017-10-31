package workers

import (
	"testing"

	"github.com/customerio/gospec"
)

// You will need to list every spec in a TestXxx method like this,
// so that gotest can be used to run the specs. Later GoSpec might
// get its own command line tool similar to gotest, but for now this
// is the way to go. This shouldn't require too much typing, because
// there will be typically only one top-level spec per class/feature.

func TestAllSpecs(t *testing.T) {
	r := gospec.NewRunner()

	r.Parallel = false

	// List all specs here
	r.AddSpec(WorkersSpec)
	r.AddSpec(ConfigSpec)
	r.AddSpec(MsgSpec)
	r.AddSpec(FetchSpec)
	r.AddSpec(WorkerSpec)
	r.AddSpec(ManagerSpec)
	r.AddSpec(ScheduledSpec)
	r.AddSpec(EnqueueSpec)
	r.AddSpec(MiddlewareSpec)
	r.AddSpec(MiddlewareRetrySpec)
	r.AddSpec(MiddlewareStatsSpec)

	// Run GoSpec and report any errors to gotest's `testing.T` instance
	gospec.MainGoTest(r, t)
}

func mkConfig(c WorkersConfig) (config *config, err error) {
	config, err = Configure(c)
	if err != nil {
		return
	}

	conn := config.Pool.Get()
	conn.Do("flushdb")
	conn.Close()

	return
}

func mkDefaultConfig() *config {
	config, err := mkConfig(WorkersConfig{
		RedisURL:  "redis://localhost:6379/0",
		ProcessID: "1",
		Namespace: "prod",
	})
	if err != nil {
		panic(err)
	}

	return config
}

func mkWorkers(config *config) *Workers {
	return NewWorkers(config)
}
