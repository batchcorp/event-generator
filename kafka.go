package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/batchcorp/schemas/build/go/events/fakes"
	"github.com/golang/protobuf/proto"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/event-generator/cli"
)

func NewKafkaWriter(address, topic string, batchSize int, insecureTLS bool) (*kafka.Writer, error) {
	dialer := &kafka.Dialer{
		Timeout: 5 * time.Second,
	}

	if insecureTLS {
		dialer.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	// The dialer timeout does not get utilized under some conditions (such as
	// when kafka is configured to NOT auto create topics) - we need a
	// mechanism to bail out early.
	ctxDeadline, _ := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))

	// Attempt to establish connection
	_, err := dialer.DialLeader(ctxDeadline, "tcp", address, topic, 0)
	if err != nil {
		return nil, fmt.Errorf("unable to create initial connection to host '%s': %s", address, err)
	}

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:   []string{address},
		Topic:     topic,
		Dialer:    dialer,
		BatchSize: batchSize,
	})

	return w, nil
}

func sendKafkaEvents(wg *sync.WaitGroup, params *cli.Params, id string, generateChan chan *fakes.Event) {
	defer wg.Done()

	id = "kafka-" + id

	logrus.Infof("worker id '%s' started", id)

	w, err := NewKafkaWriter(params.Address, params.Topic, params.BatchSize, params.DisableTLS)
	if err != nil {
		logrus.Fatalf("%s: unable to create new kafka writer: %s", id, err)
	}

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
		default:
			logrus.Fatalf("%s: unknown encoding '%s'", id, params.Encode)
		}

		if err != nil {
			logrus.Errorf("unable to marshal event to '%s': %s", err, params.Encode)
			logrus.Errorf("problem event: %+v", e)
			continue
		}

		batch = append(batch, data)

		if len(batch) >= batchSize {
			logrus.Infof("%s: batch size reached (%d); sending events", id, len(batch))

			if err := w.WriteMessages(context.Background(), toKafkaMessages(params.Topic, batch)...); err != nil {
				logrus.Errorf("%s: unable to publish %d records: %s", id, len(batch), err)
			} else {
				numEvents += len(batch)
			}

			performSleep(params)

			// Reset batch
			batch = make([][]byte, 0)

			// BatchSizeRandom batch size either up or down in size
			if params.BatchSizeRandom {
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
		}
	}

	logrus.Infof("%s: sending final batch (%d events)", id, len(batch))

	if err := w.WriteMessages(context.Background(), toKafkaMessages(params.Topic, batch)...); err != nil {
		logrus.Errorf("%s: unable to publish %d records: %s", id, len(batch), err)
	} else {
		numEvents += len(batch)
	}

	logrus.Infof("%s: finished work (sent %d events); exiting", id, numEvents)
}

func toKafkaMessages(topic string, entries [][]byte) []kafka.Message {
	messages := make([]kafka.Message, 0)

	for _, v := range entries {
		messages = append(messages, kafka.Message{
			Topic: topic,
			Value: v,
			Time:  time.Now().UTC(),
		})
	}

	return messages
}
