package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"

	"github.com/dmitryikh/tube/api"
	"github.com/dmitryikh/tube/broker"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func main() {
	// log.SetLevel(log.TraceLevel)
	log.Info("Welcome to tube!")
	config, err := broker.ReadConfig()
	if err != nil {
		panic(err)
	}
	config.LogConfig()
	topicManager, err := broker.NewTopicManager(config)
	if err != nil {
		panic(err)
	}
	service := broker.NewBrokerService(topicManager)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	api.RegisterBrokerServiceServer(grpcServer, service)

	interruptChannel := make(chan os.Signal, 1)
	signal.Notify(interruptChannel, os.Interrupt)
	go func() {
		<-interruptChannel
		grpcServer.GracefulStop()
	}()

	log.Infof("Listening on :%d", config.Port)
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Errorf("Grpc Server error: %s", err)
	}

	err = topicManager.Shutdown()
	if err != nil {
		log.Errorf("TopicsManager error: %s", err)
	}
	log.Info("Bye!")
}
