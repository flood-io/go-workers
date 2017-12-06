package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
)

func ConfigSpec(c gospec.Context) {

	c.Specify("Configure", func() {
		c.Specify("sets redis pool size which defaults to 1", func() {
			config, err := mkConfig(ConfigureOpts{
				RedisURL:  "localhost:6379",
				ProcessID: "1",
				PoolSize:  20,
			})

			c.Expect(config.Pool.MaxIdle, Equals, 20)
			c.Expect(err, IsNil)
		})

		c.Specify("can specify custom process", func() {
			config, err := mkConfig(ConfigureOpts{
				RedisURL:  "redis://localhost:6379",
				ProcessID: "2",
			})

			c.Expect(err, IsNil)
			c.Expect(config.processId, Equals, "2")
		})

		c.Specify("requires a server parameter", func() {
			_, err := mkConfig(ConfigureOpts{ProcessID: "2"})

			c.Expect(err.Error(), Equals, "workers.Configure requires RedisURL to connect to redis.")
		})

		c.Specify("requires a process parameter", func() {
			_, err := mkConfig(ConfigureOpts{RedisURL: "redis://localhost:6379"})

			c.Expect(err.Error(), Equals, "workers.Configure requires ProcessID to uniquely identify this worker process.")
		})

		c.Specify("defaults poll interval to 15 seconds", func() {
			config, err := mkConfig(ConfigureOpts{
				RedisURL:  "redis://localhost:6379",
				ProcessID: "1",
			})

			c.Expect(config.PollInterval, Equals, 15)
			c.Expect(err, IsNil)
		})

		c.Specify("allows customization of poll interval", func() {
			config, err := mkConfig(ConfigureOpts{
				RedisURL:     "redis://localhost:6379",
				ProcessID:    "1",
				PollInterval: 1,
			})

			c.Expect(config.PollInterval, Equals, 1)
			c.Expect(err, IsNil)
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
