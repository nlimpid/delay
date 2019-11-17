package delay

import (
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nlimpid/delay/server"

	"github.com/nlimpid/delay/delaypb"

	"google.golang.org/grpc"

	"github.com/sirupsen/logrus"
)

func main() {
	lis, err := net.Listen("tcp", "127.0.0.1:8001")
	if err != nil {
		logrus.Fatalf("listen err: %v", err.Error())
	}
	server := server.Server{}
	s := grpc.NewServer()
	delaypb.RegisterDelayServer(s, &server)

	go s.Serve(lis)
	logrus.Infof("server1 listen on 127.0.0.1:8001")
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM)
	signal.Notify(c, os.Interrupt)
	for _ = range c {
		logrus.Infof("捕获中断，退出服务")
		go func() {
			time.Sleep(5 * time.Second)
			s.Stop()
			os.Exit(0)
		}()
		//s.GracefulStop()
		//server.Close <- struct{}{}
		s.GracefulStop()
		os.Exit(0)
		//s.Stop()
	}
	//grpc.GracefulShutdown(s)
}
