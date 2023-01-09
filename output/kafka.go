package output

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

	"github.com/batchcorp/event-generator/params/types"
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

func SendKafkaEvents(wg *sync.WaitGroup, p *types.Params, id string, generateChan chan *fakes.Event) {
	defer wg.Done()

	id = "kafka-" + id

	logrus.Infof("worker id '%s' started", id)

	w, err := NewKafkaWriter(p.Address, p.Topic, p.XXXBatchSize, p.DisableTLS)
	if err != nil {
		logrus.Fatalf("%s: unable to create new kafka writer: %s", id, err)
	}

	batch := make([][]byte, 0)

	batchSize := p.XXXBatchSize
	numEvents := 0

	for e := range generateChan {
		var data []byte
		var err error

		switch p.Encode {
		case "json":
			data, err = json.Marshal(e)
		case "protobuf":
			data, err = proto.Marshal(e)
		default:
			logrus.Fatalf("%s: unknown encoding '%s'", id, p.Encode)
		}

		if err != nil {
			logrus.Errorf("unable to marshal event to '%s': %s", err, p.Encode)
			logrus.Errorf("problem event: %+v", e)
			continue
		}

		batch = append(batch, data)

		if len(batch) >= batchSize {
			logrus.Infof("%s: batch size reached (%d); sending events", id, len(batch))

			if err := w.WriteMessages(context.Background(), toKafkaMessages(p.Topic, batch)...); err != nil {
				logrus.Errorf("%s: unable to publish %d records: %s", id, len(batch), err)
			} else {
				numEvents += len(batch)
			}

			if p.XXXSleep > 0 {
				time.Sleep(p.XXXSleep)
			}

			// Reset batch
			batch = make([][]byte, 0)

			// BatchSizeRandom batch size either up or down in size
			if p.XXXBatchSizeMin != 0 && p.XXXBatchSizeMax != 0 {
				rand.Seed(time.Now().UnixNano())
				batchSize = rand.Intn(p.XXXBatchSizeMax-p.XXXBatchSizeMin) + 1

				logrus.Infof("Next batch size randomized to %d", batchSize)
			}
		}
	}

	logrus.Infof("%s: sending final batch (%d events)", id, len(batch))

	if err := w.WriteMessages(context.Background(), toKafkaMessages(p.Topic, batch)...); err != nil {
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
