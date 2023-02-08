package output

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

	"github.com/batchcorp/event-generator/params/types"
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

func SendRabbitMQEvents(wg *sync.WaitGroup, p *types.Params, id string, generateChan chan *fakes.Event) {
	defer wg.Done()

	id = "rabbit-" + id

	logrus.Infof("worker id '%s' started", id)

	r, err := NewRabbit(p.Address, p.RabbitExchange, p.RabbitDeclareExchange, p.RabbitDurableExchange)
	if err != nil {
		logrus.Fatalf("unable to create new rabbit instance: %s", err)
	}

	batch := make([][]byte, 0)

	batchSize := p.XXXBatchSize
	numEvents := 0
	iter := 0
	numFudgedEvents := 0
	fudgeEvery := 0

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

		if p.XXXFudgeCount != 0 && p.Encode == "json" {
			iter += 1

			if iter == fudgeEvery {
				data, err = fudge(p, data)
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

			for _, entry := range batch {
				if err := r.Publish(context.Background(), p.RabbitRoutingKey, entry); err != nil {
					logrus.Errorf("%s: unable to publish record: %s", id, err)
				} else {
					numEvents += 1
				}
			}

			if p.XXXSleep != 0 {
				time.Sleep(p.XXXSleep)
			}

			// Reset batch
			batch = make([][]byte, 0)

			if p.XXXBatchSizeMin != 0 && p.XXXBatchSizeMax != 0 {
				rand.Seed(time.Now().UnixNano())
				batchSize = rand.Intn(p.XXXBatchSizeMax-p.XXXBatchSizeMin) + 1

				logrus.Infof("Next batch size randomized to: %d", batchSize)
			}
		}
	}

	// Give publisher a chance to complete
	time.Sleep(3 * time.Second)

	logrus.Infof("%s: sending final batch (length: %d)", id, len(batch))

	for _, entry := range batch {
		if err := r.Publish(context.Background(), p.RabbitRoutingKey, entry); err != nil {
			logrus.Errorf("%s: unable to publish records: %s", id, err)
		} else {
			numEvents += 1
		}
	}

	logrus.Infof("%s: finished work (sent %d events); exiting", id, numEvents)
}
