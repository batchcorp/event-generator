package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
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
