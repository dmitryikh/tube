package broker

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/dmitryikh/tube/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

var lis *bufconn.Listener
var server *grpc.Server
var wg sync.WaitGroup

const dataDir = "test_data"

func setup() {
	lis = bufconn.Listen(bufSize)

	config := &Config{
		DataDir:                dataDir,
		SegmentMaxSizeBytes:    1024 * 1024 * 10,
		SegmentMaxSizeMessages: 10000,
	}
	topicManager, err := NewTopicManager(config)
	if err != nil {
		panic(err)
	}
	server = grpc.NewServer()
	service := NewBrokerService(topicManager)
	api.RegisterBrokerServiceServer(server, service)
	wg.Add(1)
	go func() {
		if err := server.Serve(lis); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
		if err := topicManager.Shutdown(); err != nil {
			log.Fatalf("TopicManager exit with error: %v", err)
		}
		wg.Done()
	}()
}

func bufDialer(string, time.Duration) (net.Conn, error) {
	return lis.Dial()
}

func shutdown() {
	server.GracefulStop()
	wg.Wait()
	os.RemoveAll(dataDir)
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	shutdown()
	os.Exit(code)
}

func generateMessages(startSeq uint64, topic string, num int) []*api.MessageWithRoute {
	messages := make([]*api.MessageWithRoute, 0, num)
	for i := 0; i < num; i++ {
		seq := startSeq + uint64(i)
		timestamp := uint64(time.Now().UnixNano()) + uint64(i)
		payloadStr := fmt.Sprintf("lovely payload %d/%d", seq, timestamp)
		meta := make(map[string][]byte)
		meta["ts"] = []byte(fmt.Sprintf("%d", timestamp))
		message := &api.Message{
			Seq:       seq,
			Timestamp: timestamp,
			Payload:   []byte(payloadStr),
			Meta:      meta,
		}
		messages = append(messages, &api.MessageWithRoute{
			Message: message,
			Topic:   topic,
		})
	}
	return messages
}

func checkError(t *testing.T, err error, response interface{}, methodName string) {
	if err != nil {
		t.Fatalf("%s failed: %s", methodName, err)
	}
	v := reflect.Indirect(reflect.ValueOf(response))
	erro, isOk := v.FieldByName("Error").Interface().(*api.Error)
	if !isOk {
		t.Fatalf("%s failed: bad error cast", methodName)
	}
	if erro != nil {
		t.Fatalf("%s failed: %s", methodName, erro.Message)
	}
}

func TestCreateTopic(t *testing.T) {
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	client := api.NewBrokerServiceClient(conn)

	{
		resp, err := client.CreateTopic(ctx, &api.CreateTopicsRequest{
			Topic: "demo",
		})

		checkError(t, err, resp, "CreateTopic failed")
		log.Printf("CreateTopic Response: %+v", resp)
	}

	// resp2, err := client.GetLastMessage(ctx, &api.GetLastMessageRequest{
	// 	Topic: "demo",
	// })

	// if err != nil {
	// 	t.Fatalf("GetLastMessage transport failed: %v", err)
	// }
	// if resp2.Error == nil {
	// 	t.Fatalf("GetLastMessage should contain error!")
	// }
	// log.Printf("GetLastMessage Response: %+v", resp2)

	{
		resp, err := client.ProduceBatch(ctx, &api.MessagesBatchRequest{
			Messages: generateMessages(0, "demo", 10),
		})
		checkError(t, err, resp, "ProduceBatch")
		log.Printf("ProduceBatch Response: %+v", resp)
	}

	{
		resp, err := client.GetLastMessage(ctx, &api.GetLastMessageRequest{
			Topic: "demo",
		})
		checkError(t, err, resp, "GetLastMessage")
		log.Printf("GetLastMessage Response: %+v", resp)
	}
}
