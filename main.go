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

	grpcCollectorAddressFlag = kingpin.Flag("grpc-collector-address", "where to send events").
					Default("grpc-collector.dev.batch.sh:9000").String()

	tokenFlag = kingpin.Flag("token", "Batch token").Required().String()

	countFlag = kingpin.Flag("count", "how many events to generate and send").
			Default("1").Int()

	batchSizeFlag = kingpin.Flag("batch-size", "how many events to send in a single batch").
			Default("100").Int()

	workersFlag = kingpin.Flag("workers", "how many workers to use").
			Default("1").Int()

	disableTLSFlag = kingpin.Flag("disable-tls", "disable tls").
			Default("false").
			Bool()
)

func init() {
	kingpin.CommandLine.HelpFlag.Short('h')
	kingpin.Parse()
}

func main() {
	if *workersFlag > *countFlag {
		logrus.Fatalf("worker count (%d) cannot exceed count (%d)", *workersFlag, *countFlag)
	}

	entries, err := generateEvents(*typeFlag, *countFlag)
	if err != nil {
		logrus.Fatalf("unable to generate events: %s", err)
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

			go sendEvents(wg, workerID, entries[previousIndex:])
			continue
		}

		untilIndex := previousIndex + numEventsPerWorker

		wg.Add(1)

		go sendEvents(wg, workerID, entries[previousIndex:untilIndex])

		previousIndex = untilIndex
	}

	logrus.Info("Waiting on workers to finish...")

	wg.Wait()

	logrus.Info("All work completed")

}

func sendEvents(wg *sync.WaitGroup, id string, entries []*events.Event) {
	defer wg.Done()

	logrus.Infof("worker id '%s' started with '%d' events", id, len(entries))

	conn, ctx, err := NewConnection(*grpcCollectorAddressFlag, *tokenFlag, 5*time.Second, *disableTLSFlag, true)
	if err != nil {
		logrus.Fatalf("%s: unable to establish grpc connection: %s", id, err)
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

			logrus.Infof("%s: Received status from grpc-collector: %s", id, resp.Status)
		}
	}

	logrus.Infof("%s: sending final batch (length: %d)", id, len(batch))

	resp, err := client.AddRecord(ctx, &services.GenericRecordRequest{
		Records: toGenericRecords(batch),
	})

	if err != nil {
		logrus.Errorf("%s: unable to add records: %s", id, err)
	}

	logrus.Infof("%s: Received final status from grpc-collector: %s", id, resp.Status)
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
