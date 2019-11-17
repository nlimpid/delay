package server

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"

	any "github.com/golang/protobuf/ptypes/any"
	"github.com/nlimpid/delay/delaypb"
)

func (s *Server) AddTask(ctx context.Context, req *delaypb.AddTaskReq) (*delaypb.AddTaskRsp, error) {
	err := s.Queue.Add(req.Task.Topic, req.Task.Id, req.DelayTime, req.Task.Params.Value)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *Server) Notify(stream delaypb.Delay_NotifyServer) error {
	go func() {
		for {
			message, err := stream.Recv()
			if err != nil {
				continue
			}
			if message.S == delaypb.Status_success {
				fmt.Printf("delete something")
			}
		}
	}()

	for {
		task := <-s.Queue.Ch
		err := stream.Send(&delaypb.NotifyRsp{
			Task: &delaypb.Task{
				Id:    task.ID,
				Topic: task.Topic,
				Params: &any.Any{
					Value: task.Value,
				},
			},
		})
		logrus.Errorf("send message: %+v err: %v", task, err)
	}
}
