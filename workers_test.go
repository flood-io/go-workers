package workers

import (
	"reflect"

	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
)

var called chan bool

func myJob(message *Msg) error {
	called <- true
	return nil
}

func WorkersSpec(c gospec.Context) {
	c.Specify("Workers", func() {
		config := mkDefaultConfig()
		w := mkWorkers(config)

		c.Specify("allows running in tests", func() {
			called = make(chan bool)

			w.Process("myqueue", myJob, 10)

			w.Start()

			w.Enqueue("myqueue", "Add", []int{1, 2})
			<-called

			w.Quit()
		})

		// TODO make this test more deterministic, randomly locks up in travis.
		//c.Specify("allows starting and stopping multiple times", func() {
		//	called = make(chan bool)

		//	Process("myqueue", myJob, 10)

		//	Start()
		//	Quit()

		//	Start()

		//	Enqueue("myqueue", "Add", []int{1, 2})
		//	<-called

		//	Quit()
		//})

		c.Specify("runs beforeStart hooks", func() {
			hooks := []string{}

			w.BeforeStart(func() {
				hooks = append(hooks, "1")
			})
			w.BeforeStart(func() {
				hooks = append(hooks, "2")
			})
			w.BeforeStart(func() {
				hooks = append(hooks, "3")
			})

			w.Start()

			c.Expect(reflect.DeepEqual(hooks, []string{"1", "2", "3"}), IsTrue)

			w.Quit()
		})

		c.Specify("runs beforeStart hooks", func() {
			hooks := []string{}

			w.DuringDrain(func() {
				hooks = append(hooks, "1")
			})
			w.DuringDrain(func() {
				hooks = append(hooks, "2")
			})
			w.DuringDrain(func() {
				hooks = append(hooks, "3")
			})

			w.Start()

			c.Expect(reflect.DeepEqual(hooks, []string{}), IsTrue)

			w.Quit()

			c.Expect(reflect.DeepEqual(hooks, []string{"1", "2", "3"}), IsTrue)
		})
	})
}
