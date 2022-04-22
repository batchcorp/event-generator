package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/batchcorp/event-generator/cli"
	"github.com/batchcorp/schemas/build/go/events/fakes"
	"github.com/golang/protobuf/proto"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
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

func NewKafkaConn(address string, disableTLS bool) (*kafka.Conn, error) {
	dialer := &kafka.Dialer{
		Timeout: 5 * time.Second,
	}

	if disableTLS {
		dialer.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	// The dialer timeout does not get utilized under some conditions (such as
	// when kafka is configured to NOT auto create topics) - we need a
	// mechanism to bail out early.
	ctxDeadline, _ := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))

	// Attempt to establish connection
	conn, err := dialer.DialContext(ctxDeadline, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("unable to create initial connection to host '%s': %s", address, err)
	}

	return conn, nil
}

func createKafkaTopics(wg *sync.WaitGroup, params *cli.Params, id string, entries []*fakes.Event, sleepTime time.Duration) {
	//defer wg.Done()
	//
	//id = "kafka-" + id
	//
	//logrus.Infof("worker id '%s' started with '%d' events", id, len(entries))
	//
	//c, err := NewKafkaConn(params.Address, params.DisableTLS)
	//if err != nil {
	//	logrus.Fatalf("%s: unable to create new kafka writer: %s", id, err)
	//}
	//
	//// We already get only a small chunk of topics, no need to batch anything
	//for _, entry := range entries {
	//
	//	if err := c.CreateTopics(kafka.TopicConfig{
	//		Topic:             entry.TopicName,
	//		NumPartitions:     entry.TopicPartitionCount,
	//		ReplicationFactor: entry.TopicReplicaCount,
	//	}); err != nil {
	//		logrus.Errorf("unable to create topic '%s': %s", entry.TopicName, err)
	//	}
	//
	//	time.Sleep(sleepTime)
	//}
	//
	//logrus.Infof("%s: finished work; exiting", id)
}

func sendKafkaEvents(wg *sync.WaitGroup, params *cli.Params, id string, entries []*fakes.Event, sleepTime time.Duration) {
	defer wg.Done()

	id = "kafka-" + id

	logrus.Infof("worker id '%s' started with '%d' events", id, len(entries))

	w, err := NewKafkaWriter(params.Address, params.Topic, params.BatchSize, params.DisableTLS)
	if err != nil {
		logrus.Fatalf("%s: unable to create new kafka writer: %s", id, err)
	}

	batch := make([][]byte, 0)

	batchSize := params.BatchSize

	for _, e := range entries {
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
				logrus.Errorf("%s: unable to publish records: %s", id, err)
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
		}
	}

	logrus.Infof("%s: sending final batch (length: %d)", id, len(batch))

	if err := w.WriteMessages(context.Background(), toKafkaMessages(params.Topic, batch)...); err != nil {
		logrus.Errorf("%s: unable to publish records: %s", id, err)
	}

	logrus.Infof("%s: finished work; exiting", id)
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
