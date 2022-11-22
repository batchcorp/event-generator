package main

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/batchcorp/schemas/build/go/events/fakes"
	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/batchcorp/event-generator/cli"

	"github.com/batchcorp/event-generator/events"
)

const (
	OutputGRPCCollector = "batch-grpc-collector"
	OutputKafka         = "kafka"
	OutputRabbitMQ      = "rabbitmq"
)

var (
	params = &cli.Params{}
)

func init() {
	kingpin.Flag("type", "type of event(s) to generate").
		Required().
		EnumVar(&params.Type, "all", "monitoring", "billing", "audit", "search", "reset_password", "topic_test")

	kingpin.Flag("topic-prefix", "What prefix to use for new topics (only used if 'type' is topic_test)").
		Default("test").
		StringVar(&params.TopicPrefix)

	kingpin.Flag("topic-replicas", "TopicReplicaCount count configuration for topic (only used if 'type' is topic_test)").
		Default("1").
		IntVar(&params.TopicReplicas)

	kingpin.Flag("topic-partitions", "TopicPartitionCount count configuration for topic (only used if 'type' is topic_test)").
		Default("1").
		IntVar(&params.TopicPartitions)

	kingpin.Flag("token", "Batch token").
		StringVar(&params.Token)

	kingpin.Flag("count", "how many events to generate and send").
		Default("1").
		IntVar(&params.Count)

	kingpin.Flag("encode", "encode the event as JSON (default) or protobuf").
		Default("json").
		EnumVar(&params.Encode, "json", "protobuf")

	kingpin.Flag("batch-size", "how many events to send in a single batch").
		Default("100").
		IntVar(&params.BatchSize)

	kingpin.Flag("workers", "how many workers to use").
		Default("1").
		IntVar(&params.Workers)

	kingpin.Flag("disable-tls", "disable tls").
		Default("false").
		BoolVar(&params.DisableTLS)

	kingpin.Flag("address", "where to send events").
		Default("grpc-collector.dev.batch.sh:9000").
		StringVar(&params.Address)

	kingpin.Flag("output", "what kind of destination is this").
		Default("batch-grpc-collector").
		EnumVar(&params.Output, OutputGRPCCollector, OutputKafka, OutputRabbitMQ)

	kingpin.Flag("topic", "topic to write events to (kafka-only)").
		StringVar(&params.Topic)

	kingpin.Flag("rabbit-exchange", "which exchange to write to").
		StringVar(&params.RabbitExchange)

	kingpin.Flag("rabbit-routing-key", "what routing key to use when writing data").
		StringVar(&params.RabbitRoutingKey)

	kingpin.Flag("rabbit-declare-exchange", "whether to declare exchange").
		BoolVar(&params.RabbitDeclareExchange)

	kingpin.Flag("rabbit-durable-exchange", "whether the exchange should be durable").
		BoolVar(&params.RabbitDeclareExchange)

	kingpin.Flag("fudge", "number of events that should be fudged/broken (only for JSON)").
		IntVar(&params.Fudge)

	kingpin.Flag("sleep", "sleep for $INPUT milliseconds between batches").
		Default("0").
		IntVar(&params.Sleep)

	kingpin.Flag("sleep-random", "sleep for $random milliseconds between batches").
		Default("0").
		IntVar(&params.SleepRandom)

	kingpin.Flag("batch-size-random", "randomize the size of batches").
		Default("false").
		BoolVar(&params.BatchSizeRandom)

	kingpin.CommandLine.HelpFlag.Short('h')
	kingpin.Parse()
}

func main() {
	if err := validateFlags(); err != nil {
		logrus.Fatalf("unable to validate flags: %s", err)
	}

	logrus.Infof("Generating '%d' event(s)...", params.Count)

	generateChan, err := events.GenerateEvents(params)
	if err != nil {
		logrus.Fatalf("unable to generate events: %s", err)
	}

	// Set appropriate func
	var sendEventsFunc func(wg *sync.WaitGroup, params *cli.Params, id string, generateChan chan *fakes.Event)

	switch params.Output {
	case OutputKafka:
		sendEventsFunc = sendKafkaEvents
	case OutputGRPCCollector:
		sendEventsFunc = sendGRPCEvents
	case OutputRabbitMQ:
		sendEventsFunc = sendRabbitMQEvents
	default:
		logrus.Fatalf("unknown output flag '%s'", params.Output)
	}

	wg := &sync.WaitGroup{}

	for i := 0; i < params.Workers; i++ {
		workerID := fmt.Sprintf("worker-%d", i)

		logrus.Infof("Launching worker '%s'", workerID)

		wg.Add(1)

		//noinspection GoNilness
		go sendEventsFunc(wg, params, workerID, generateChan)
	}

	logrus.Info("Waiting on workers to finish...")

	wg.Wait()

	logrus.Info("All work completed")

}

func validateFlags() error {
	if params.Workers >= params.Count {
		return fmt.Errorf("worker count (%d) cannot exceed count (%d)", params.Workers, params.Count)
	}

	if params.Output == OutputKafka {
		if params.Topic == "" {
			return errors.New("topic must be set when using kafka output")
		}
	}

	if params.Output == OutputGRPCCollector {
		if params.Token == "" {
			return errors.New("token must be set when using batch-grpc-collector output")
		}
	}

	if params.Type == OutputRabbitMQ {
		if params.RabbitExchange == "" {
			return errors.New("--rabbit-exchange cannot be empty with rabbit output")
		}

		if params.RabbitRoutingKey == "" {
			return errors.New("--rabbit-routing-key cannot be empty with rabbit output")
		}
	}

	if params.Fudge > params.Count {
		return errors.New("fudge value cannot exceed count")
	}

	return nil
}

func performSleep(params *cli.Params) {
	if params.Sleep != 0 {
		sleepTime := time.Duration(params.Sleep) * time.Millisecond

		logrus.Infof("sleeping for '%s' before working on next batch", sleepTime)

		time.Sleep(sleepTime)
	} else if params.SleepRandom != 0 {
		rand.Seed(time.Now().UnixNano())
		sleepTime := time.Duration(rand.Intn(params.SleepRandom-1)+1) * time.Millisecond

		logrus.Infof("sleeping for '%s' before working on next batch", sleepTime)

		time.Sleep(time.Duration(rand.Intn(params.SleepRandom-1)+1) * time.Millisecond)
	}
}
