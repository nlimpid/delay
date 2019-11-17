package queue

import (
	"fmt"
	"testing"
	"time"
)

func TestDelayQueue_Run(t *testing.T) {
	d := &DelayQueue{
		Bucket: [3600][]Task{},
		Begin: time.Now(),
		Done:   nil,
	}
	d.Run()
}



func TestDelayQueue_UT(t *testing.T) {
	d := &DelayQueue{
		Bucket: [3600][]Task{},
		Begin: time.Now(),
		Done:   nil,
	}

	for i := 10; i < 20; i++ {
		topic := fmt.Sprintf("%v", i)
		d.Add(topic, int64(i))
	}


	d.Run()


}
