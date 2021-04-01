package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/event-generator/events"
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
