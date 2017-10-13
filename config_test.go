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

	c.Specify("Configure", func() {
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
	})

	c.Specify("ConfigureFromURLString", func() {
		c.Specify("parses url", func() {
			config := ConfigureFromURLString("redis://localhost:6379/0?pool=24&poll_interval=42&namespace=spacedname&process=2")

			c.Expect(config.PollInterval, Equals, 42)
			c.Expect(config.Namespace(), Equals, "spacedname")
			c.Expect(config.processId, Equals, "2")
		})

		c.Specify("parses url, overriding with extraOptions", func() {
			extraOptions := map[string]string{"process": "321"}
			config := ConfigureFromURLStringAndOverrides("redis://localhost:6379/0?pool=24&poll_interval=42&namespace=spacedname&process=123", extraOptions)

			c.Expect(config.PollInterval, Equals, 42)
			c.Expect(config.Namespace(), Equals, "spacedname")
			c.Expect(config.processId, Equals, "321")
		})
	})

	c.Specify("NamespacedKey", func() {
		config := mkDefaultConfig()

		c.Specify("normalises namespace colons", func() {
			config.SetNamespace("prod")
			c.Expect(config.NamespacedKey("queue"), Equals, "prod:queue")

			config.SetNamespace("prod:")
			c.Expect(config.NamespacedKey("queue"), Equals, "prod:queue")

			config.SetNamespace("")
			c.Expect(config.NamespacedKey("queue"), Equals, "queue")
		})

		c.Specify("joins multiple elements", func() {
			config.SetNamespace("prod")
			c.Expect(config.NamespacedKey("queue", "thing"), Equals, "prod:queue:thing")

			config.SetNamespace("")
			c.Expect(config.NamespacedKey("queue", "thing"), Equals, "queue:thing")
		})
	})

	c.Specify("TrimKeyNamespace", func() {
		config := mkDefaultConfig()

		c.Specify("normalises namespace colons", func() {
			config.SetNamespace("prod")
			c.Expect(config.TrimKeyNamespace("prod:queue"), Equals, "queue")

			config.SetNamespace("prod:")
			c.Expect(config.TrimKeyNamespace("prod:queue"), Equals, "queue")

			config.SetNamespace("")
			c.Expect(config.NamespacedKey("prod:queue"), Equals, "prod:queue")
		})
	})
}
