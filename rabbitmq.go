package main

import (
	"context"
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"github.com/batchcorp/rabbit"
	"github.com/batchcorp/schemas/build/go/events/fakes"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	"github.com/batchcorp/event-generator/cli"
)

func NewRabbit(address, exchange string, declare, durable bool) (rabbit.IRabbit, error) {
	r, err := rabbit.New(&rabbit.Options{
		URLs: []string{address},
		Mode: rabbit.Producer,
		Bindings: []rabbit.Binding{
			{
				ExchangeName:    exchange,
				ExchangeType:    amqp.ExchangeTopic,
				ExchangeDeclare: declare,
				ExchangeDurable: durable,
			},
		},
		RetryReconnectSec: rabbit.DefaultRetryReconnectSec,
		AppID:             "event-generator",
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new rabbit backend")
	}

	return r, nil
}

func sendRabbitMQEvents(wg *sync.WaitGroup, params *cli.Params, id string, generateChan chan *fakes.Event, sleepTime time.Duration) {
	defer wg.Done()

	id = "rabbit-" + id

	logrus.Infof("worker id '%s' started", id)

	r, err := NewRabbit(params.Address, params.RabbitExchange, params.RabbitDeclareExchange, params.RabbitDurableExchange)
	if err != nil {
		logrus.Fatalf("unable to create new rabbit instance: %s", err)
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

			for _, entry := range batch {
				if err := r.Publish(context.Background(), params.RabbitRoutingKey, entry); err != nil {
					logrus.Errorf("%s: unable to publish record: %s", id, err)
				} else {
					numEvents += 1
				}
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

	// Give publisher a chance to complete
	time.Sleep(3 * time.Second)

	logrus.Infof("%s: sending final batch (length: %d)", id, len(batch))

	for _, entry := range batch {
		if err := r.Publish(context.Background(), params.RabbitRoutingKey, entry); err != nil {
			logrus.Errorf("%s: unable to publish records: %s", id, err)
		} else {
			numEvents += 1
		}
	}

	logrus.Infof("%s: finished work (sent %d events); exiting", id, numEvents)
}
