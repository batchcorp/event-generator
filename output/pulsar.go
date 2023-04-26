package output

import (
	"context"
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/batchcorp/schemas/build/go/events/fakes"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/event-generator/params/types"
)

func NewPulsarProducer(address, topic string) (pulsar.Client, pulsar.Producer, error) {
	c, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: address,
	})

	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to create pulsar client")
	}

	p, err := c.CreateProducer(pulsar.ProducerOptions{
		Topic:               topic,
		BatchingMaxMessages: 10_000,
		SendTimeout:         -1,
	})

	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to create pulsar producer")
	}

	return c, p, nil
}

func SendPulsarEvents(wg *sync.WaitGroup, p *types.Params, id string, generateChan chan *fakes.Event) {
	defer wg.Done()

	id = "pulsar-" + id

	logrus.Infof("worker id '%s' started", id)

	client, producer, err := NewPulsarProducer(p.Address, p.Topic)
	if err != nil {
		logrus.Fatalf("%s: unable to create new pulsar writer: %s", id, err)
	}

	if p.PulsarCreateSubscription {
		if err := createPulsarSubscription(client, producer, p.Topic); err != nil {
			logrus.Fatalf("%s: unable to create pulsar subscription: %s", id, err)
		}
	}

	defer producer.Close()
	defer client.Close()

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

			if err := pulsarProduceMessages(p, producer, batch); err != nil {
				logrus.Errorf("%s: unable to publish %d records: %s", id, len(batch), err)
			}

			numEvents += len(batch)

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

	if err := pulsarProduceMessages(p, producer, batch); err != nil {
		logrus.Errorf("%s: unable to publish final %d records: %s", id, len(batch), err)
	}

	numEvents += len(batch)

	logrus.Infof("%s: finished work (sent %d events); exiting", id, numEvents)
}

func createPulsarSubscription(client pulsar.Client, producer pulsar.Producer, topic string) error {
	// The pulsar go library does not have any management functions, so we have
	// to create the subscription via consumer
	if _, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "eg-dummy-subscription-" + topic,
		Type:             pulsar.Shared,
	}); err != nil {
		return errors.Wrap(err, "unable to create pulsar dummy subscription")
	}

	return nil
}

func pulsarProduceMessages(p *types.Params, producer pulsar.Producer, batch [][]byte) error {
	for _, v := range batch {
		if p.PulsarAsyncProducer {
			producer.SendAsync(context.Background(), &pulsar.ProducerMessage{
				Payload: v,
			}, pulsarCallback)

		} else {
			_, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
				Payload: v,
			})

			if err != nil {
				return errors.Wrap(err, "unable to send message")
			}
		}
	}

	return nil
}

func pulsarCallback(id pulsar.MessageID, msg *pulsar.ProducerMessage, err error) {
	if err != nil {
		logrus.Errorf("unable to send message: %s", err)
		return
	}
}
