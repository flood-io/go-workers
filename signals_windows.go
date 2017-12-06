package workers

import (
	"os"
	"os/signal"
	"syscall"
)

func (w *Workers) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	for sig := range signals {
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			w.Quit()
		}
	}
}
