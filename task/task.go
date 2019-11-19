package task

import "time"

type StorageS interface {
	AddTask(task Task) error
	GetTaskContent(key string) (Task, error)
	Delete(key string)  error
	Restore() ([3600][]Task, error)
}

//func NewStorage() StorageS {
//	return &storage.BoltDB{}
//}


type Task struct {
	// unique id
	ID string
	// Round * 3600 + Index = delta
	Round int64
	Index int64
	// epoch should exec
	AbsoluteTime int64
	AbsTime time.Time

	Topic string
	Value []byte
}

