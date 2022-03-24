package main

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/batchcorp/event-generator/cli"
	"github.com/batchcorp/schemas/build/go/events/fakes"
	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/batchcorp/event-generator/events"
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
		EnumVar(&params.Output, "batch-grpc-collector", "kafka")

	kingpin.Flag("topic", "topic to write events to (kafka-only)").
		StringVar(&params.Topic)

	kingpin.Flag("sleep", "how long to sleep, in milliseconds, between batches").
		Default("0").
		IntVar(&params.Sleep)

	kingpin.Flag("randomize", "randomize the size of batches").
		Default("false").
		BoolVar(&params.Randomize)

	kingpin.CommandLine.HelpFlag.Short('h')
	kingpin.Parse()
}

func main() {
	if err := validateFlags(); err != nil {
		logrus.Fatalf("unable to validate flags: %s", err)
	}

	logrus.Infof("Generating '%d' event(s)...", params.Count)

	sleepTime, _ := time.ParseDuration(fmt.Sprintf("%dms", params.Sleep))

	entries, err := events.GenerateEvents(params)
	if err != nil {
		logrus.Fatalf("unable to generate events: %s", err)
	}

	// Set appropriate func
	var sendEventsFunc func(wg *sync.WaitGroup, params *cli.Params, id string, entries []*fakes.Event, sleepTime time.Duration)

	switch params.Output {
	case "kafka":
		if params.Type == "topic_test" {
			sendEventsFunc = createKafkaTopics
		} else {
			sendEventsFunc = sendKafkaEvents
		}
	case "batch-grpc-collector":
		sendEventsFunc = sendGRPCEvents
	default:
		logrus.Fatalf("unknown output flag '%s'", params.Output)
	}

	// Assign work to workers
	numEventsPerWorker := params.Count / params.Workers

	var previousIndex int

	wg := &sync.WaitGroup{}

	for i := 0; i < params.Workers; i++ {
		workerID := fmt.Sprintf("worker-%d", i)

		logrus.Infof("Launching worker '%s'", workerID)

		// Last worker gets remainder
		if i == (params.Workers - 1) {
			wg.Add(1)

			//noinspection GoNilness
			go sendEventsFunc(wg, params, workerID, entries[previousIndex:], sleepTime)
			continue
		}

		untilIndex := previousIndex + numEventsPerWorker

		wg.Add(1)

		//noinspection GoNilness
		go sendEventsFunc(wg, params, workerID, entries[previousIndex:untilIndex], sleepTime)

		previousIndex = untilIndex
	}

	logrus.Info("Waiting on workers to finish...")

	wg.Wait()

	logrus.Info("All work completed")

}

func validateFlags() error {
	if params.Workers >= params.Count {
		return fmt.Errorf("worker count (%d) cannot exceed count (%d)", params.Workers, params.Count)
	}

	if params.Output == "kafka" {
		if params.Type != "topic_test" && params.Topic == "" {
			return errors.New("topic must be set when using kafka output")
		}
	}

	if params.Output == "batch-grpc-collector" {
		if params.Token == "" {
			return errors.New("token must be set when using batch-grpc-collector output")
		}
	}

	return nil
}
