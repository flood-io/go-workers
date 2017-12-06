package workers

func (w *Workers) Ping() error {
	conn := w.config.Pool.Get()
	defer conn.Close()

	_, err := conn.Do("PING")
	return err
}
