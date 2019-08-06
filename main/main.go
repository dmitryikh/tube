package main

import (
	"fmt"
	"log"
	"net"

	"github.com/dmitryikh/tube/api"
	"github.com/dmitryikh/tube/broker"
	"google.golang.org/grpc"
)

func main() {
	fmt.Printf("Hello!")
	config, err := broker.ReadConfig()
	if err != nil {
		panic(err)
	}
	topicManager, err := broker.NewTopicManager(config)
	if err != nil {
		panic(err)
	}
	service := broker.NewBrokerService(topicManager)

	lis, err := net.Listen("tcp", ":8010")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	api.RegisterBrokerServiceServer(grpcServer, service)
	err = grpcServer.Serve(lis)
	// log.Fatal(err)

	err = topicManager.Shutdown()
	log.Fatal(err)
}
