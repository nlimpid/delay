package queue

import (
	"fmt"
	"github.com/nlimpid/delay/storage"
	"github.com/nlimpid/delay/task"
	"github.com/rs/xid"
	"time"
)


type DelayQueue struct {
	Bucket [3600][]task.Task
	Begin time.Time
	Done chan bool
	Cursor int

	S task.StorageS
}


func New() (*DelayQueue, error) {
	s, err := storage.Init()
	if err != nil {
		panic(err)
	}

	buckets, err := s.Restore()
	if err != nil {
		return nil, err
	}

	now := time.Now()
	return &DelayQueue{
		Bucket: buckets,
		Begin:  now,
		Done:   nil,

		S: s,
	}, nil
}

func (d *DelayQueue) Run() {
	cursor := 0
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-d.Done:
			fmt.Println("Done!")
			return
		case t := <-ticker.C:
			// cursor move 1 when 1 seconds
			cursor+=1
			if cursor == 3600 {
				cursor = 0
			}
			fmt.Println("Current time: ", t)
			interval := t.Sub(d.Begin).Seconds()
			fmt.Printf("interval: %v\n", interval)

			if len(d.Bucket[d.Cursor]) > 0 {
				for _, v := range d.Bucket[d.Cursor] {
					go Exec(v)
				}

			}
		}

	}
}

func Exec(task task.Task) {
	fmt.Printf("exec job: %s", task.Topic)
	return
}

func (d *DelayQueue) Add(topic string, absTime int64, value []byte) error {
	delayTime := absTime - time.Now().Unix()
	index := delayTime % 3600
	round := delayTime / 3600
	id := xid.New()

	task := task.Task{
		ID:           id.String(),
		Round:        round,
		Index:        index,
		AbsoluteTime: absTime,
		Topic: topic,
		Value: value,
	}

	err := d.S.AddTask(task)
	if err != nil {
		return err
	}

	d.Bucket[index] = append(d.Bucket[index], task)
	return nil
}

