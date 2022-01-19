package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/batchcorp/schemas/build/go/events/records"
	"github.com/batchcorp/schemas/build/go/services"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	"github.com/batchcorp/event-generator/events"
)

func NewGRPCConnection(address, token string, timeout time.Duration, disableTLS, noCtx bool) (*grpc.ClientConn, context.Context, error) {
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

func sendGRPCEvents(wg *sync.WaitGroup, id string, entries []*events.Event, sleepTime time.Duration) {
	defer wg.Done()

	id = "gRPC-" + id

	logrus.Infof("worker id '%s' started with '%d' events", id, len(entries))

	conn, ctx, err := NewGRPCConnection(*addressFlag, *tokenFlag, 5*time.Second, *disableTLSFlag, true)
	if err != nil {
		logrus.Fatalf("%s: unable to establish gRPC connection: %s", id, err)
	}

	client := services.NewGRPCCollectorClient(conn)

	batch := make([][]byte, 0)

	batchSize := *batchSizeFlag

	for _, e := range entries {
		jsonData, err := json.Marshal(e)
		if err != nil {
			logrus.Errorf("unable to marshal event to json: %s", err)
			logrus.Errorf("problem event: %+v", e)
			continue
		}

		batch = append(batch, jsonData)

		if len(batch) >= batchSize {
			logrus.Infof("%s: batch size reached (%d); sending events", id, len(batch))

			resp, err := client.AddRecord(ctx, &services.GenericRecordRequest{
				Records: toGenericRecords(batch),
			})

			if err != nil {
				logrus.Errorf("%s: unable to add records: %s", id, err)
			}

			time.Sleep(sleepTime)

			// Reset batch
			batch = make([][]byte, 0)

			// Randomize batch size either up or down in size
			if *randomizeFlag == true {
				randomizer := rand.New(rand.NewSource(time.Now().UnixNano()))
				fudgeFactor := randomizer.Intn(*batchSizeFlag / 5)

				if fudgeFactor%2 == 0 {
					logrus.Infof("Fudging UP by %d", fudgeFactor)
					batchSize = *batchSizeFlag + fudgeFactor
				} else {
					logrus.Infof("Fudging DOWN by %d", fudgeFactor)
					batchSize = *batchSizeFlag - fudgeFactor
				}
			}

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
