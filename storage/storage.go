package storage

import (
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/nlimpid/delay/task"
	"time"
)


type BoltDB struct {
	db *bolt.DB
}


var BucketTime = "time"
var BucketContent = "content"

func Init() (task.StorageS, error) {
	var err error
	db, err := bolt.Open("my.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}
	return &BoltDB{
		db: db,
	}, nil
}

func (b *BoltDB) AddTask(task task.Task) error {
	err := b.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BucketContent))
		err := b.Put([]byte(task.ID), task.Value)
		if err != nil {
			return err
		}

		bt := tx.Bucket([]byte(BucketTime))
		err = bt.Put([]byte(task.ID), []byte(task.AbsTime.String()))
		return err
	})
	if err != nil {
		return err
	}
	return nil
}


func (b *BoltDB) GetTaskContent(key string) (task.Task, error) {
	var task task.Task
	err := b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BucketContent))
		v := b.Get([]byte(key))
		// using fast decode
		json.Unmarshal(v, &task)
		return nil
	})
	return task, err
}

func (b *BoltDB) Delete(key string)  error {
	err := b.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BucketTime))
		err := b.Delete([]byte(key))
		if err != nil {
			return err
		}
		bt := tx.Bucket([]byte(BucketContent))
		err = bt.Delete([]byte(key))
		if err != nil {
			return err
		}
		return nil
	})
	return err
}



// Restore: only restore time
func (b *BoltDB) Restore() ([3600][]task.Task, error) {
	now := time.Now()
	bucket :=[3600][]task.Task{}
	err := b.db.View(func(tx *bolt.Tx) error {
		bt := tx.Bucket([]byte(BucketTime))
		bt.ForEach(func(k, v []byte) error {
			absTime, _ := time.Parse( "2006-01-02 15:04:05.999999999 -0700 MST", string(v))
			delayTime := int64(absTime.Sub(now))
			// TODO: more precisely
			if delayTime <= 10 {
				// exec
			}
			index := delayTime % 3600
			round := delayTime / 3600

			task := task.Task{
				ID:           string(k),
				Round:        round,
				Index:        index,
				AbsTime: absTime,
			}

			bucket[index] = append(bucket[index], task)

			fmt.Printf("key=%s, value=%s\n", k, v)
			return nil
		})
		return nil
	})
	if err != nil {
		return bucket, err
	}
	return bucket, nil
}


