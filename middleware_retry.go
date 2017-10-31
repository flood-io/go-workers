package workers

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"
)

const (
	DEFAULT_MAX_RETRY = 25
	LAYOUT            = "2006-01-02 15:04:05 MST"
)

type MiddlewareRetry struct {
	config *config
}

func (r *MiddlewareRetry) Call(queue string, message *Msg, next func() error) (err error) {
	err = next()

	if err != nil {
		if retry(message) {
			conn := r.config.Pool.Get()
			defer conn.Close()

			message.Set("queue", queue)
			message.Set("error_message", fmt.Sprintf("%v", err))
			retryCount := incrementRetry(message)
			err = nil

			waitDuration := durationToSecondsWithNanoPrecision(
				time.Duration(
					secondsToDelay(retryCount),
				) * time.Second,
			)

			_, err = conn.Do(
				"zadd",
				r.config.NamespacedKey(RETRY_KEY),
				nowToSecondsWithNanoPrecision()+waitDuration,
				message.ToJson(),
			)

			// If we can't add the job to the retry queue,
			// then we shouldn't return the error, otherwise
			// it'll disappear into the void.
			if err != nil {
				log.Printf("failed to add job to retry %v", err)
				err = nil
			}
		}
	}

	return
}

func retry(message *Msg) bool {
	retry := false
	max := DEFAULT_MAX_RETRY

	if param, err := message.Get("retry").Bool(); err == nil {
		retry = param
	} else if param, err := message.Get("retry").Int(); err == nil {
		max = param
		retry = true
	}

	count, _ := message.Get("retry_count").Int()

	return retry && count < max
}

func incrementRetry(message *Msg) (retryCount int) {
	retryCount = 0

	if count, err := message.Get("retry_count").Int(); err != nil {
		message.Set("failed_at", time.Now().UTC().Format(LAYOUT))
	} else {
		message.Set("retried_at", time.Now().UTC().Format(LAYOUT))
		retryCount = count + 1
	}

	message.Set("retry_count", retryCount)

	return
}

func secondsToDelay(count int) int {
	power := math.Pow(float64(count), 4)
	return int(power) + 15 + (rand.Intn(30) * (count + 1))
}
