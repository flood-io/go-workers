package workers

func (w *Workers) Namespace() string {
	return w.config.Namespace()
}

func (w *Workers) NamespacedKey(keys ...string) string {
	return w.config.NamespacedKey(keys...)
}

func (w *Workers) TrimKeyNamespace(key string) string {
	return w.config.TrimKeyNamespace(key)
}
