package queue

import (
	"fmt"
	"time"
)


// TODO: resistent
var lastTime = time.Unix(1,1)
var curSor int

type Task struct {
	ID int64
	Round int64
	Index int64
	ExecTime int64

	Topic string

}

type DelayQueue struct {
	Bucket [3600][]Task
	Begin time.Time
	Done chan bool
	Cursor int
}


func New() *DelayQueue {

	now := time.Now()
	delta := int(now.Sub(lastTime).Seconds())

	
	return &DelayQueue{
		Bucket: [3600][]Task{},
		Begin:  now,
		Done:   nil,
		Cursor: curSor+delta,
	}
	

}


func (d *DelayQueue) Run() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-d.Done:
			fmt.Println("Done!")
			return
		case t := <-ticker.C:
			// cursor move 1 when 1 seconds
			d.Cursor += 1
			curSor+=1
			fmt.Println("Current time: ", t)
			interval := t.Sub(d.Begin).Seconds()
			fmt.Printf("interval: %v\n", interval)
			d.Begin = t
			lastTime = t


			if len(d.Bucket[d.Cursor]) > 0 {
				for _, v := range d.Bucket[d.Cursor] {
					go Exec(v)
				}

			}
		}
	}
}

func Exec(task Task) {
	fmt.Printf("exec job: %s", task.Topic)
	return
}

func (d *DelayQueue) Add(topic string, delayTime int64) error {
	index := delayTime % 3600
	round := delayTime / 3600


	task := Task{
		Round:    round,
		Index:    index,
		//ExecTime: d.Beg,
		Topic: topic,
	}

	d.Bucket[index] = append(d.Bucket[index], task)
	return nil
}

func getID(index, round int64) int64 {
	return round * 3600 + index

}


//func (d *DelayQueue) Del(id int64) error {
//	index := id % 3600
//
//	d.Bucket[id]
//
//}