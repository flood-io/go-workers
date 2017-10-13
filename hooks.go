package workers

func (w *Workers) BeforeStart(f func()) {
	w.access.Lock()
	defer w.access.Unlock()
	w.beforeStart = append(w.beforeStart, f)
}

// func AfterStart
// func BeforeQuit
// func AfterQuit

func (w *Workers) DuringDrain(f func()) {
	w.access.Lock()
	defer w.access.Unlock()
	w.duringDrain = append(w.duringDrain, f)
}

func runHooks(hooks []func()) {
	for _, f := range hooks {
		f()
	}
}
