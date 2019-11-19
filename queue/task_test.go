package queue

import (
	"fmt"
	"github.com/nlimpid/delay/task"
	"testing"
	"time"
)

func TestDelayQueue_Run(t *testing.T) {
	d := &DelayQueue{
		Bucket: [3600][]task.Task{},
		Begin: time.Now(),
		Done:   nil,
	}
	d.Run()
}



func TestDelayQueue_UT(t *testing.T) {
	q, err := New()
	if err != nil {
		t.Errorf("t: %v", err)
		return
	}


	for i := 10; i < 20; i++ {
		topic := fmt.Sprintf("%v", i)
		tim := time.Now().Add(10000 * time.Second)
		q.Add(topic, tim, []byte("test"))
	}


	q.Run()

}



func TestDelayQueue_Restore(t *testing.T) {
	q, err := New()
	if err != nil {
		t.Errorf("t: %v", err)
		return
	}

	for _, v := range q.Bucket {
		for _, tas := range v {
			t.Logf("id: %v, tasabs:  %v, value: %v\n", tas.ID, tas.AbsTime, tas.Value)
		}
	}

}

