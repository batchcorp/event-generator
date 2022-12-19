package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/batchcorp/collector-schemas/build/go/protos/records"
	"github.com/batchcorp/collector-schemas/build/go/protos/services"
	"github.com/batchcorp/schemas/build/go/events/fakes"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/sjson"
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

func sendGRPCEvents(wg *sync.WaitGroup, params *cli.Params, id string, generateChan chan *fakes.Event) {
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
	iter := 0
	numFudgedEvents := 0
	fudgeEvery := 0

	// Figure out how often to fudge
	if params.Fudge != 0 {
		fudgeEvery = params.Count / params.Fudge
	}

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

		if params.Fudge != 0 && params.Encode == "json" {
			iter += 1

			if iter == fudgeEvery {
				data, err = fudge(params, data)
				if err != nil {
					panic("unable to fudge: " + err.Error())
				}

				iter = 0
				numFudgedEvents += 1
			}
		}

		batch = append(batch, data)

		if len(batch) >= batchSize {
			logrus.Infof("%s: batch size reached (%d); sending events", id, len(batch))

			resp, err := client.AddRecord(ctx, &services.GenericRecordRequest{
				Records: toGenericRecords(batch),
			})

			if err != nil {
				if strings.Contains(err.Error(), "unauthorized") {
					logrus.Fatal("Received 'unauthorized' from grpc-collector - exiting")
				}

				logrus.Errorf("%s: unable to add records: %s", id, err)
				continue
			} else {
				numEvents += len(batch)
			}

			performSleep(params)

			// Reset batch
			batch = make([][]byte, 0)

			// BatchSizeRandom batch size either up or down in size
			if params.BatchSizeRandom {
				randomizer := rand.New(rand.NewSource(time.Now().UnixNano()))

				var fudgeFactor int

				// Prevent a panic by never passing 0 to Intn()
				if params.BatchSize/5 != 0 {
					fudgeFactor = randomizer.Intn(params.BatchSize / 5)
				}

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

	logrus.Infof("%s: finished work (sent: %d, fudged: %d); exiting", id, numEvents, numFudgedEvents)
}

func fudge(params *cli.Params, jsonBytes []byte) ([]byte, error) {
	var (
		value interface{}
		err   error
	)

	switch params.FudgeType {
	case "int":
		value, err = strconv.ParseInt(params.FudgeValue, 10, 64)
	case "bool":
		value, err = strconv.ParseBool(params.FudgeValue)
	case "string":
		value = params.FudgeValue
	default:
		return nil, fmt.Errorf("unrecognized fudge type '%s'", params.FudgeType)
	}

	data, err := sjson.Set(string(jsonBytes), params.FudgeField, value)
	if err != nil {
		return nil, errors.Wrap(err, "unable to fudge input json")
	}

	return []byte(data), nil
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
