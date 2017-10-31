package workers

import (
	"fmt"
	"time"
)

type MiddlewareLogging struct{}

func (l *MiddlewareLogging) Call(queue string, message *Msg, next func() error) (err error) {
	prefix := fmt.Sprint(queue, " JID-", message.Jid())

	start := time.Now()
	Logger.Println(prefix, "start")
	Logger.Println(prefix, "args:", message.Args().ToJson())

	err = next()
	if err != nil {
		Logger.Println(prefix, "fail:", time.Since(start))

		// buf := make([]byte, 4096)
		// buf = buf[:runtime.Stack(buf, false)]
		// Logger.Printf("%s error: %v\n%s", prefix, err, buf)

		Logger.Printf("%s error: %v", prefix, err)
	}

	Logger.Println(prefix, "done:", time.Since(start))

	return
}
