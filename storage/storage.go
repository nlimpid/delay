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
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(BucketTime))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte(BucketContent))
		if err != nil {
			return err
		}
		return nil
	})
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
		err = bt.Put([]byte(task.ID), []byte(task.AbsTime.Format(time.RFC3339Nano)))
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
		c := bt.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			absTime, _ := time.Parse( time.RFC3339Nano, string(v))
			delayTime := int64(absTime.Sub(now))
			// TODO: more precisely
			if delayTime <= 10 {
				fmt.Printf("need to exec: %v, %v\n", delayTime, string(k))
				// exec
				continue
			}
			index := delayTime % 3600
			round := delayTime / 3600

			ta := task.Task{
				ID:           string(k),
				Round:        round,
				Index:        index,
				AbsTime: absTime,
			}


				bucket[index] = append(bucket[index], ta)

			fmt.Printf("key=%s, value=%s\n", k, v)
		}
		return nil
	})
	if err != nil {
		return bucket, err
	}
	return bucket, nil
}


