package workers

type Action interface {
	Call(queue string, message *Msg, next func() error) error
}

type Middlewares struct {
	actions []Action
}

func (m *Middlewares) Append(action Action) {
	m.actions = append(m.actions, action)
}

func (m *Middlewares) Prepend(action Action) {
	actions := make([]Action, len(m.actions)+1)
	actions[0] = action
	copy(actions[1:], m.actions)
	m.actions = actions
}

func (m *Middlewares) Equals(other *Middlewares) bool {
	if len(m.actions) != len(other.actions) {
		return false
	}

	for i := range m.actions {
		if m.actions[i] != other.actions[i] {
			return false
		}
	}

	return true
}

func (m *Middlewares) AppendToCopy(mids []Action) *Middlewares {
	return NewMiddleware(append(m.actions, mids...)...)
}

func (m *Middlewares) call(queue string, message *Msg, final func() error) error {
	return continuation(m.actions, queue, message, final)()
}

func continuation(actions []Action, queue string, message *Msg, final func() error) func() error {
	return func() (err error) {
		if len(actions) > 0 {
			err = actions[0].Call(
				queue,
				message,
				continuation(actions[1:], queue, message, final),
			)

			return
		} else {
			return final()
		}
	}
}

func NewMiddleware(actions ...Action) *Middlewares {
	return &Middlewares{actions}
}
