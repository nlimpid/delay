package server

import "github.com/nlimpid/delay/rqueue"

type Server struct {
	Queue rqueue.RDelayQueue
}
