package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
)

func ConfigSpec(c gospec.Context) {
	var recoverOnPanic = func(f func()) (err interface{}) {
		defer func() {
			if cause := recover(); cause != nil {
				err = cause
			}
		}()

		f()

		return
	}

	c.Specify("sets redis pool size which defaults to 1", func() {
		config := mkConfig(map[string]string{
			"server":  "localhost:6379",
			"process": "1",
			"pool":    "20",
		})

		c.Expect(config.Pool.MaxIdle, Equals, 20)
	})

	c.Specify("can specify custom process", func() {
		config := mkConfig(map[string]string{
			"server":  "localhost:6379",
			"process": "2",
		})

		c.Expect(config.processId, Equals, "2")
	})

	c.Specify("requires a server parameter", func() {
		err := recoverOnPanic(func() {
			mkConfig(map[string]string{"process": "2"})
		})

		c.Expect(err, Equals, "Configure requires a 'server' option, which identifies a Redis instance")
	})

	c.Specify("requires a process parameter", func() {
		err := recoverOnPanic(func() {
			mkConfig(map[string]string{"server": "localhost:6379"})
		})

		c.Expect(err, Equals, "Configure requires a 'process' option, which uniquely identifies this instance")
	})

	c.Specify("adds ':' to the end of the namespace", func() {
		config := Configure(map[string]string{
			"server":    "localhost:6379",
			"process":   "1",
			"namespace": "prod",
		})

		c.Expect(config.Namespace, Equals, "prod:")
	})

	c.Specify("defaults poll interval to 15 seconds", func() {
		config := mkConfig(map[string]string{
			"server":  "localhost:6379",
			"process": "1",
		})

		c.Expect(config.PollInterval, Equals, 15)
	})

	c.Specify("allows customization of poll interval", func() {
		config := mkConfig(map[string]string{
			"server":        "localhost:6379",
			"process":       "1",
			"poll_interval": "1",
		})

		c.Expect(config.PollInterval, Equals, 1)
	})
}
