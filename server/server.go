package server

import (
	"github.com/nlimpid/delay/queue"
	"github.com/nlimpid/delay/rqueue"
)

type Server struct {
	Q *queue.DelayQueue
	Queue rqueue.RDelayQueue
}


func NewServer() (*Server, error) {

	q, err := queue.New()
	if err != nil {
		return nil, err
	}

	s := &Server{
		Q:     q,
		Queue: rqueue.RDelayQueue{},
	}
	return s, nil


}
