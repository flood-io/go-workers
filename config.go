package workers

import (
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

type WorkersConfig struct {
	RedisURL     string
	ProcessID    string
	PoolSize     int
	PollInterval int
	Namespace    string
}

type config struct {
	processId          string
	PollInterval       int
	Pool               *redis.Pool
	Fetch              func(queue string) Fetcher
	GlobalMiddlewares  *Middlewares
	namespace          string
	namespaceWithColon string
}

func Configure(cfg WorkersConfig) (configObj *config) {
	configObj = &config{
		cfg.ProcessID,
		cfg.PollInterval,
		&redis.Pool{
			MaxIdle:     cfg.PoolSize,
			IdleTimeout: 240 * time.Second,
			Dial: func() (redis.Conn, error) {
				c, err := redis.DialURL(cfg.RedisURL)
				if err != nil {
					return nil, err
				}
				return c, err
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		},
		nil,
		nil,
		"",
		"",
	}

	configObj.SetNamespace(cfg.Namespace)

	configObj.GlobalMiddlewares = newDefaultMiddlewares(configObj)

	// closes over configObj
	configObj.Fetch = func(queue string) Fetcher {
		return NewFetch(configObj, queue, make(chan *Msg), make(chan bool))
	}

	return
}

func (c *config) Namespace() string {
	return c.namespace
}

func (c *config) SetNamespace(newNamespace string) {
	if newNamespace == "" {
		c.namespace = ""
		c.namespaceWithColon = ""
	} else {
		c.namespace = strings.TrimSuffix(newNamespace, ":")
		c.namespaceWithColon = c.namespace + ":"
	}
}

func (c *config) NamespacedKey(keys ...string) string {
	return c.namespaceWithColon + strings.Join(keys, ":")
}

func (c *config) TrimKeyNamespace(key string) string {
	return strings.TrimPrefix(key, c.namespaceWithColon)
}
