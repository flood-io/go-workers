package workers

import (
	"os"
	"testing"

	"github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
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

const defaultRedisURL = "redis://localhost:6379/0"

func redisURL() string {
	url := os.Getenv("REDIS_URL")

	if url == "" {
		return defaultRedisURL
	}

	return url
}

func mkConfig(c ConfigureOpts) (config *config, err error) {
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
	config, err := mkConfig(ConfigureOpts{
		RedisURL:  redisURL(),
		ProcessID: "1",
		Namespace: "prod",
	})
	if err != nil {
		panic(err)
	}

	return config
}

func mkMockConfig(conn redis.Conn) *config {
	config, err := mkConfig(ConfigureOpts{
		RedisPool: mkMockRedisPool(conn),
		ProcessID: "1",
		Namespace: "prod",
	})
	if err != nil {
		panic(err)
	}

	return config
}

func mkMockRedisPool(conn redis.Conn) (redisPool *redis.Pool) {
	redisPool = &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return conn, nil
		},
	}

	return
}

func mkWorkers(config *config) *Workers {
	return NewWorkers(config)
}

var _ redis.Conn = (*NullMockRedisConn)(nil)

type NullMockRedisConn struct {
}

// Close closes the connection.
func (m *NullMockRedisConn) Close() error {
	return nil
}

// Err returns a non-nil value when the connection is not usable.
func (m *NullMockRedisConn) Err() error {
	return nil
}

// Do sends a command to the server and returns the received reply.
func (m *NullMockRedisConn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	return nil, nil
}

// Send writes the command to the client's output buffer.
func (m *NullMockRedisConn) Send(commandName string, args ...interface{}) error {
	return nil
}

// Flush flushes the output buffer to the Redis server.
func (m *NullMockRedisConn) Flush() error {
	return nil
}

// Receive receives a single reply from the Redis server
func (m *NullMockRedisConn) Receive() (reply interface{}, err error) {
	return nil, nil
}

var _ redis.Conn = (*FuncMockRedisConn)(nil)

type FuncMockRedisConn struct {
	CloseFn   func() error
	ErrFn     func() error
	DoFn      func(commandName string, args ...interface{}) (reply interface{}, err error)
	SendFn    func(commandName string, args ...interface{}) error
	FlushFn   func() error
	ReceiveFn func() (reply interface{}, err error)
}

// Close closes the connection.
func (m *FuncMockRedisConn) Close() error {
	if m.CloseFn != nil {
		return m.CloseFn()
	}
	return nil
}

// Err returns a non-nil value when the connection is not usable.
func (m *FuncMockRedisConn) Err() error {
	if m.ErrFn != nil {
		return m.ErrFn()
	}
	return nil
}

// Do sends a command to the server and returns the received reply.
func (m *FuncMockRedisConn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	if m.DoFn != nil {
		return m.DoFn(commandName, args...)
	}
	return nil, nil
}

// Send writes the command to the client's output buffer.
func (m *FuncMockRedisConn) Send(commandName string, args ...interface{}) error {
	if m.SendFn != nil {
		return m.SendFn(commandName, args...)
	}
	return nil
}

// Flush flushes the output buffer to the Redis server.
func (m *FuncMockRedisConn) Flush() error {
	if m.FlushFn != nil {
		return m.FlushFn()
	}
	return nil
}

// Receive receives a single reply from the Redis server
func (m *FuncMockRedisConn) Receive() (reply interface{}, err error) {
	if m.ReceiveFn != nil {
		return m.ReceiveFn()
	}
	return nil, nil
}
