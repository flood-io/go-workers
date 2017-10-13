package workers

import (
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

type config struct {
	processId          string
	namespace          string
	namespaceWithColon string
	PollInterval       int
	Pool               *redis.Pool
	Fetch              func(queue string) Fetcher
	GlobalMiddlewares  *Middlewares
}

func ConfigureFromURLStringAndOverrides(urlString string, extraOptions map[string]string) (configObj *config) {
	url, err := url.Parse(urlString)
	if err != nil {
		panic("Unable to parse redis url")
	}

	if url.Scheme != "redis" {
		panic("Config url scheme must be 'redis'")
	}

	query := url.Query()

	options := map[string]string{
		"server":        url.Host,
		"database":      url.Path,
		"process":       query.Get("process"),
		"pool":          query.Get("pool"),
		"poll_interval": query.Get("poll_interval"),
		"namespace":     query.Get("namespace"),
	}

	for k, v := range extraOptions {
		options[k] = v
	}

	return Configure(options)
}

func ConfigureFromURLString(urlString string) (configObj *config) {
	return ConfigureFromURLStringAndOverrides(urlString, map[string]string{})
}

func Configure(options map[string]string) (configObj *config) {
	var poolSize int
	var pollInterval int

	if options["server"] == "" {
		panic("Configure requires a 'server' option, which identifies a Redis instance")
	}
	if options["process"] == "" {
		panic("Configure requires a 'process' option, which uniquely identifies this instance")
	}
	if options["pool"] == "" {
		options["pool"] = "1"
	}
	if seconds, err := strconv.Atoi(options["poll_interval"]); err == nil {
		pollInterval = seconds
	} else {
		pollInterval = 15
	}

	poolSize, _ = strconv.Atoi(options["pool"])

	configObj = &config{
		options["process"],
		"",
		"",
		pollInterval,
		&redis.Pool{
			MaxIdle:     poolSize,
			IdleTimeout: 240 * time.Second,
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", options["server"])
				if err != nil {
					return nil, err
				}
				if options["password"] != "" {
					if _, err := c.Do("AUTH", options["password"]); err != nil {
						c.Close()
						return nil, err
					}
				}
				if options["database"] != "" {
					if _, err := c.Do("SELECT", options["database"]); err != nil {
						c.Close()
						return nil, err
					}
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
	}

	configObj.SetNamespace(options["namespace"])

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
