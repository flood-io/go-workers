package workers

import (
	"errors"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	defaultRetryQueue         = "goretry"
	defaultScheduledJobsQueue = "schedule"
)

type ConfigureOpts struct {
	// RedisURL is a redis schemed URL as understood by redigo:
	// https://godoc.org/github.com/garyburd/redigo/redis#DialURL
	RedisURL string

	// ProcessID uniquely identifies this process. Used for uncoordinated reliable processing of messages.
	ProcessID string

	// MaxIdle is the maximum number of idle connections to keep in the redis connection pool.
	MaxIdle int

	// PoolSize is the maximum number of connections allowed by the redis conneciton pool.
	PoolSize int

	// PollInterval is how often we should poll for scheduled jobs.
	PollInterval int

	// Namespace is the namespace to use for redis keys.
	Namespace string

	RedisPool *redis.Pool
}

type config struct {
	processId          string
	PollInterval       int
	Pool               *redis.Pool
	Fetch              func(queue string) Fetcher
	GlobalMiddlewares  *Middlewares
	namespace          string
	namespaceWithColon string

	retryQueue         string
	scheduledJobsQueue string
}

func Configure(cfg ConfigureOpts) (configObj *config, err error) {
	var redisPool *redis.Pool

	if cfg.RedisPool != nil {
		redisPool = cfg.RedisPool
	} else {
		redisPool, err = initRedisPool(cfg)
		if err != nil {
			return
		}
	}

	if cfg.ProcessID == "" {
		err = errors.New("workers.Configure requires ProcessID to uniquely identify this worker process.")
		return
	}

	if cfg.PollInterval == 0 {
		cfg.PollInterval = 15
	}

	configObj = &config{
		processId:          cfg.ProcessID,
		PollInterval:       cfg.PollInterval,
		Pool:               redisPool,
		retryQueue:         defaultRetryQueue,
		scheduledJobsQueue: defaultScheduledJobsQueue,
	}

	configObj.SetNamespace(cfg.Namespace)

	configObj.GlobalMiddlewares = newDefaultMiddlewares(configObj)

	// closes over configObj
	configObj.Fetch = func(queue string) Fetcher {
		return NewFetch(configObj, queue, make(chan *Msg), make(chan bool))
	}

	return
}

func initRedisPool(cfg ConfigureOpts) (redisPool *redis.Pool, err error) {
	if cfg.RedisURL == "" {
		err = errors.New("workers.Configure requires RedisURL to connect to redis.")
		return
	}

	if cfg.MaxIdle == 0 {
		cfg.MaxIdle = cfg.PoolSize
	}

	redisPool = &redis.Pool{
		MaxIdle:     cfg.MaxIdle,
		MaxActive:   cfg.PoolSize,
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
