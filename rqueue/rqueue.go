package rqueue

import (
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/sirupsen/logrus"
)

const (
	cursorKey = "delay-cursor"
	lastime   = "delay-lastime"
	roundKey  = "delay-round"
	//hSet
)

type RTask struct {
	ID       string
	Round    int64
	Index    int64
	ExecTime int64

	Topic string
	Value []byte
}

type BTask struct {
	ID [16]byte
}

type RDelayQueue struct {
	Bucket  [3600][]RTask
	BucketB [3600][]byte
	Begin   time.Time
	Done    chan bool
	Cursor  int64
	Round   int64
	Ch      chan *RTask

	rc redis.Client
}

func New() (*RDelayQueue, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	now := time.Now()
	lastTimeStr, err := client.Get(lastime).Result()
	if err != nil {
		return nil, err
	}
	lastTimeI, _ := strconv.ParseInt(lastTimeStr, 10, 64)
	lastTime := time.Unix(lastTimeI, 0)
	delta := int64(now.Sub(lastTime).Seconds())

	deltaIndex := delta % 3600
	deltaRound := delta / 3600

	curIndex, _ := client.Get(cursorKey).Int64()
	curRound, _ := client.Get(roundKey).Int64()

	return &RDelayQueue{
		//Bucket: [3600][]Task{},
		Begin:  now,
		Done:   nil,
		Cursor: curIndex + deltaIndex,
		Round:  curRound + deltaRound,
	}, nil

}

func (d *RDelayQueue) Run() {

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-d.Done:
			fmt.Println("Done!")
			return
		case t := <-ticker.C:
			// cursor move 1 when 1 seconds
			if d.Cursor == 3599 {
				d.Cursor = 0
				err := d.rc.Set(cursorKey, 0, 0).Err()
				if err != nil {
					// TODO: backoff to memory
					logrus.Errorf("reset cursor err: %v", err)
				}
			}
			d.Cursor += 1
			_, err := d.rc.Incr(cursorKey).Result()
			if err != nil {
				logrus.Errorf("incr cursor err: %v", err)
			}
			fmt.Println("Current time: ", t)
			interval := t.Sub(d.Begin).Seconds()
			fmt.Printf("interval: %v\n", interval)
			d.Begin = t
			d.rc.Set(lastime, t, 0)

			// get index
			indexS := strconv.FormatInt(d.Cursor, 10)
			// manipulate redis
			value, err := d.rc.HGetAll(indexS + "round").Result()
			if len(value) > 0 {
				for k, v := range value {
					ro, _ := strconv.ParseInt(v, 10, 64)
					if ro <= 0+d.Round {
						// get value
						rsp, err := d.rc.HGet(indexS, k).Result()
						if err != nil {
							logrus.Errorf("get err: %v", err)
							return
						}
						go d.ExecRaw(rsp, k)
						d.rc.HDel(indexS)
					} else {
						d.rc.HIncrBy(indexS+"round", k, -1)
					}
				}
			}

			if len(d.Bucket[d.Cursor]) > 0 {

				for k, v := range d.Bucket[d.Cursor] {
					if v.Round == 0 {
						go d.Exec(v)
						d.rc.HDel(indexS, v.ID)
					}
					if v.Round > 0 {
						d.Bucket[d.Cursor][k].Round -= 1
						// TODO: handle bitarray
						d.rc.HIncrBy(indexS+"round", v.ID, -1)
					}
				}

			}
		}
	}
}

func (d *RDelayQueue) ExecRaw(s string, id string) {
	t := RTask{
		ID:    id,
		Topic: "",
		Value: []byte(s),
	}
	fmt.Printf("exec task id: %v, index: %v", t.ID, t.Index)

	d.Ch <- &t

}

func (d *RDelayQueue) Exec(t RTask) {
	fmt.Printf("exec task id: %v, index: %v", t.ID, t.Index)

	d.Ch <- &t

}

func (d *RDelayQueue) Add(topic string, id string, delayTime int64, value []byte) error {
	index := delayTime % 3600
	round := delayTime / 3600

	task := RTask{
		Round: round,
		Index: index,
		//ExecTime: d.Beg,
		Topic: topic,
	}

	d.Bucket[index] = append(d.Bucket[index], task)

	indexS := strconv.FormatInt(index, 10)
	d.rc.HSet(indexS+"round", id, round)
	d.rc.HSet(indexS, id, value)
	return nil
}

func getIndex(id string) int64 {
	return 0
}

func (d *RDelayQueue) Del(id string) error {
	index := getIndex(id)
	indexS := strconv.FormatInt(d.Cursor, 10)

	for k, v := range d.Bucket[index] {
		if v.ID == id {
			d.Bucket[index] = append(d.Bucket[index][:k], d.Bucket[index][k+1:]...)
		}
	}
	d.rc.HDel(indexS, id)
	return nil

}
