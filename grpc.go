package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/batchcorp/collector-schemas/build/go/protos/records"
	"github.com/batchcorp/collector-schemas/build/go/protos/services"
	"github.com/batchcorp/schemas/build/go/events/fakes"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	"github.com/batchcorp/event-generator/cli"
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

func sendGRPCEvents(wg *sync.WaitGroup, params *cli.Params, id string, generateChan chan *fakes.Event, sleepTime time.Duration) {
	defer wg.Done()

	id = "gRPC-" + id

	logrus.Infof("worker id '%s' started", id)

	conn, ctx, err := NewGRPCConnection(params.Address, params.Token, 5*time.Second, params.DisableTLS, true)
	if err != nil {
		logrus.Fatalf("%s: unable to establish gRPC connection: %s", id, err)
	}

	client := services.NewGRPCCollectorClient(conn)

	batch := make([][]byte, 0)

	batchSize := params.BatchSize
	numEvents := 0

	for e := range generateChan {
		var data []byte
		var err error

		switch params.Encode {
		case "json":
			data, err = json.Marshal(e)
		case "protobuf":
			data, err = proto.Marshal(e)
		}

		if err != nil {
			logrus.Errorf("unable to marshal event to %s: %s", params.Encode, err)
			logrus.Errorf("problem event: %+v", e)
			continue
		}

		batch = append(batch, data)

		if len(batch) >= batchSize {
			logrus.Infof("%s: batch size reached (%d); sending events", id, len(batch))

			resp, err := client.AddRecord(ctx, &services.GenericRecordRequest{
				Records: toGenericRecords(batch),
			})

			if err != nil {
				logrus.Errorf("%s: unable to add records: %s", id, err)
			} else {
				numEvents += len(batch)
			}

			time.Sleep(sleepTime)

			// Reset batch
			batch = make([][]byte, 0)

			// Randomize batch size either up or down in size
			if params.Randomize {
				randomizer := rand.New(rand.NewSource(time.Now().UnixNano()))
				fudgeFactor := randomizer.Intn(params.BatchSize / 5)

				if fudgeFactor%2 == 0 {
					logrus.Infof("Fudging UP by %d", fudgeFactor)
					batchSize = params.BatchSize + fudgeFactor
				} else {
					logrus.Infof("Fudging DOWN by %d", fudgeFactor)
					batchSize = params.BatchSize - fudgeFactor
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
	} else {
		numEvents += len(batch)
	}

	logrus.Infof("%s: finished work (sent %d events); exiting", id, numEvents)
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
