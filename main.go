package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/batchcorp/schemas/build/go/events/records"
	"github.com/batchcorp/schemas/build/go/services"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/batchcorp/event-generator/events"
)

var (
	typeFlag = kingpin.Flag("type", "type of event(s) to generate").
			Required().
			Enum("all", "monitoring", "billing", "audit", "search", "reset_password")

	tokenFlag = kingpin.Flag("token", "Batch token").
			String()

	countFlag = kingpin.Flag("count", "how many events to generate and send").
			Default("1").Int()

	batchSizeFlag = kingpin.Flag("batch-size", "how many events to send in a single batch").
			Default("100").Int()

	workersFlag = kingpin.Flag("workers", "how many workers to use").
			Default("1").Int()

	disableTLSFlag = kingpin.Flag("disable-tls", "disable tls").
			Default("false").
			Bool()

	addressFlag = kingpin.Flag("address", "where to send events").
			Default("grpc-collector.dev.batch.sh:9000").
			String()

	outputFlag = kingpin.Flag("output", "what kind of destination is this").
			Default("batch-grpc-collector").
			Enum("batch-grpc-collector", "kafka")

	topicFlag = kingpin.Flag("topic", "topic to write events to (kafka-only)").
			String()
)

func init() {
	kingpin.CommandLine.HelpFlag.Short('h')
	kingpin.Parse()
}

func validateFlags() error {
	if *workersFlag > *countFlag {
		return fmt.Errorf("worker count (%d) cannot exceed count (%d)", *workersFlag, *countFlag)
	}

	if *outputFlag == "kafka" {
		if *topicFlag == "" {
			return errors.New("topic must be set when using kafka output")
		}
	}

	if *outputFlag == "batch-grpc-collector" {
		if *tokenFlag == "" {
			return errors.New("token must be set when using batch-grpc-collector output")
		}
	}

	return nil
}

func main() {
	if err := validateFlags(); err != nil {
		logrus.Fatalf("unable to validate flags: %s", err)
	}

	logrus.Infof("Generating '%d' event(s)...", *countFlag)

	entries, err := generateEvents(*typeFlag, *countFlag)
	if err != nil {
		logrus.Fatalf("unable to generate events: %s", err)
	}

	// Set appropriate func
	var sendEventsFunc func(wg *sync.WaitGroup, id string, entries []*events.Event)

	switch *outputFlag {
	case "kafka":
		sendEventsFunc = sendKafkaEvents
	case "batch-grpc-collector":
		sendEventsFunc = sendGRPCEvents
	default:
		logrus.Fatalf("unknown output flag '%s'", *outputFlag)
	}

	// Assign work to workers
	numEventsPerWorker := *countFlag / *workersFlag

	var previousIndex int

	wg := &sync.WaitGroup{}

	for i := 0; i < *workersFlag; i++ {
		workerID := fmt.Sprintf("worker-%d", i)

		logrus.Infof("Launching worker '%s'", workerID)

		// Last worker gets remainder
		if i == (*workersFlag - 1) {
			wg.Add(1)

			//noinspection GoNilness
			go sendEventsFunc(wg, workerID, entries[previousIndex:])
			continue
		}

		untilIndex := previousIndex + numEventsPerWorker

		wg.Add(1)

		//noinspection GoNilness
		go sendEventsFunc(wg, workerID, entries[previousIndex:untilIndex])

		previousIndex = untilIndex
	}

	logrus.Info("Waiting on workers to finish...")

	wg.Wait()

	logrus.Info("All work completed")

}

func sendKafkaEvents(wg *sync.WaitGroup, id string, entries []*events.Event) {
	defer wg.Done()

	id = "kafka-" + id

	logrus.Infof("worker id '%s' started with '%d' events", id, len(entries))

	w, err := NewKafkaWriter(*addressFlag, *topicFlag, *batchSizeFlag, *disableTLSFlag)
	if err != nil {
		logrus.Fatalf("%s: unable to create new kafka writer: %s", id, err)
	}

	batch := make([][]byte, 0)

	for _, e := range entries {
		jsonData, err := json.Marshal(e)
		if err != nil {
			logrus.Errorf("unable to marshal event to json: %s", err)
			logrus.Errorf("problem event: %+v", e)
			continue
		}

		batch = append(batch, jsonData)

		if len(batch) >= *batchSizeFlag {
			logrus.Infof("%s: batch size reached (%d); sending events", id, len(batch))

			if err := w.WriteMessages(context.Background(), toKafkaMessages(batch)...); err != nil {
				logrus.Errorf("%s: unable to publish records: %s", id, err)
			}

			// Reset batch
			batch = make([][]byte, 0)
		}
	}

	logrus.Infof("%s: sending final batch (length: %d)", id, len(batch))

	if err := w.WriteMessages(context.Background(), toKafkaMessages(batch)...); err != nil {
		logrus.Errorf("%s: unable to publish records: %s", id, err)
	}

	logrus.Infof("%s: finished work; exiting", id)
}

func toKafkaMessages(entries [][]byte) []kafka.Message {
	messages := make([]kafka.Message, 0)

	for _, v := range entries {
		messages = append(messages, kafka.Message{
			Topic: *topicFlag,
			Value: v,
			Time:  time.Now().UTC(),
		})
	}

	return messages
}

func sendGRPCEvents(wg *sync.WaitGroup, id string, entries []*events.Event) {
	defer wg.Done()

	id = "gRPC-" + id

	logrus.Infof("worker id '%s' started with '%d' events", id, len(entries))

	conn, ctx, err := NewConnection(*addressFlag, *tokenFlag, 5*time.Second, *disableTLSFlag, true)
	if err != nil {
		logrus.Fatalf("%s: unable to establish gRPC connection: %s", id, err)
	}

	client := services.NewGRPCCollectorClient(conn)

	batch := make([][]byte, 0)

	for _, e := range entries {
		jsonData, err := json.Marshal(e)
		if err != nil {
			logrus.Errorf("unable to marshal event to json: %s", err)
			logrus.Errorf("problem event: %+v", e)
			continue
		}

		batch = append(batch, jsonData)

		if len(batch) >= *batchSizeFlag {
			logrus.Infof("%s: batch size reached (%d); sending events", id, len(batch))

			resp, err := client.AddRecord(ctx, &services.GenericRecordRequest{
				Records: toGenericRecords(batch),
			})

			if err != nil {
				logrus.Errorf("%s: unable to add records: %s", id, err)
			}

			// Reset batch
			batch = make([][]byte, 0)

			logrus.Infof("%s: Received status from gRPC-collector: %s", id, resp.Status)
		}
	}

	logrus.Infof("%s: sending final batch (length: %d)", id, len(batch))

	if _, err := client.AddRecord(ctx, &services.GenericRecordRequest{
		Records: toGenericRecords(batch),
	}); err != nil {
		logrus.Errorf("%s: unable to add records: %s", id, err)
	}

	logrus.Infof("%s: finished work; exiting", id)
}

func toGenericRecords(entries [][]byte) []*records.GenericRecord {
	genericRecords := make([]*records.GenericRecord, 0)

	for _, jsonData := range entries {
		genericRecords = append(genericRecords, &records.GenericRecord{
			Body:      jsonData,
			Source:    "event-generator",
			Timestamp: time.Now().UTC().Unix(),
		})
	}

	return genericRecords
}

func generateEvents(eventType string, count int) ([]*events.Event, error) {
	data := make([]*events.Event, 0)

	switch eventType {
	case string(events.BillingEventType):
		return nil, errors.New("not implemented")
		//data = events.GenerateBillingEvents(count)
	case string(events.MonitoringEventType):
		return nil, errors.New("not implemented")
		//data = events.GenerateMonitoringEvents(count)
	case string(events.AuditEventType):
		return nil, errors.New("not implemented")
		//data = events.GenerateAuditEvents(count)
	case string(events.SearchEventType):
		data = events.GenerateSearchEvents(count)
	case string(events.ForgotPasswordType):
		return nil, errors.New("not implemented")
		//data = events.GenerateForgotPasswordEvents(count)
	default:
		return nil, fmt.Errorf("unknown event type '%s'", eventType)
	}

	return data, nil
}

func NewConnection(address, token string, timeout time.Duration, disableTLS, noCtx bool) (*grpc.ClientConn, context.Context, error) {
	opts := []grpc.DialOption{
		grpc.WithBlock(),
	}

	if !disableTLS {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(
			&tls.Config{
				InsecureSkipVerify: true,
			},
		)))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}

	dialContext, _ := context.WithTimeout(context.Background(), timeout)

	conn, err := grpc.DialContext(dialContext, address, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to connect to grpc address '%s': %s", address, err)
	}

	var ctx context.Context

	if !noCtx {
		ctx, _ = context.WithTimeout(context.Background(), timeout)
	} else {
		ctx = context.Background()
	}

	md := metadata.Pairs("batch-token", token)
	outCtx := metadata.NewOutgoingContext(ctx, md)

	return conn, outCtx, nil
}
