package main

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/batchcorp/event-generator/events"
)

var (
	typeFlag = kingpin.Flag("type", "type of event(s) to generate").
			Required().
			Enum("all", "monitoring", "billing", "audit", "search", "reset_password")

	tokenFlag = kingpin.Flag("token", "Batch token").
			String()

	countFlag = kingpin.Flag("count", "how many events to generate and send").
			Default("1").Int()

	batchSizeFlag = kingpin.Flag("batch-size", "how many events to send in a single batch").
			Default("100").Int()

	workersFlag = kingpin.Flag("workers", "how many workers to use").
			Default("1").Int()

	disableTLSFlag = kingpin.Flag("disable-tls", "disable tls").
			Default("false").
			Bool()

	addressFlag = kingpin.Flag("address", "where to send events").
			Default("grpc-collector.dev.batch.sh:9000").
			String()

	outputFlag = kingpin.Flag("output", "what kind of destination is this").
			Default("batch-grpc-collector").
			Enum("batch-grpc-collector", "kafka")

	topicFlag = kingpin.Flag("topic", "topic to write events to (kafka-only)").
			String()

	sleepFlag = kingpin.Flag("sleep", "how long to sleep, in milliseconds, between batches").
			Default("0").
			Int()

	randomizeFlag = kingpin.Flag("randomize", "randomize the size of batches").
			Default("true").
			Bool()
)

func init() {
	kingpin.CommandLine.HelpFlag.Short('h')
	kingpin.Parse()
}

func main() {
	if err := validateFlags(); err != nil {
		logrus.Fatalf("unable to validate flags: %s", err)
	}

	logrus.Infof("Generating '%d' event(s)...", *countFlag)

	sleepTime, _ := time.ParseDuration(fmt.Sprintf("%dms", *sleepFlag))

	entries, err := events.GenerateEvents(*typeFlag, *countFlag)
	if err != nil {
		logrus.Fatalf("unable to generate events: %s", err)
	}

	// Set appropriate func
	var sendEventsFunc func(wg *sync.WaitGroup, id string, entries []*events.Event, sleepTime time.Duration)

	switch *outputFlag {
	case "kafka":
		sendEventsFunc = sendKafkaEvents
	case "batch-grpc-collector":
		sendEventsFunc = sendGRPCEvents
	default:
		logrus.Fatalf("unknown output flag '%s'", *outputFlag)
	}

	// Assign work to workers
	numEventsPerWorker := *countFlag / *workersFlag

	var previousIndex int

	wg := &sync.WaitGroup{}

	for i := 0; i < *workersFlag; i++ {
		workerID := fmt.Sprintf("worker-%d", i)

		logrus.Infof("Launching worker '%s'", workerID)

		// Last worker gets remainder
		if i == (*workersFlag - 1) {
			wg.Add(1)

			//noinspection GoNilness
			go sendEventsFunc(wg, workerID, entries[previousIndex:], sleepTime)
			continue
		}

		untilIndex := previousIndex + numEventsPerWorker

		wg.Add(1)

		//noinspection GoNilness
		go sendEventsFunc(wg, workerID, entries[previousIndex:untilIndex], sleepTime)

		previousIndex = untilIndex
	}

	logrus.Info("Waiting on workers to finish...")

	wg.Wait()

	logrus.Info("All work completed")

}

func validateFlags() error {
	if *workersFlag > *countFlag {
		return fmt.Errorf("worker count (%d) cannot exceed count (%d)", *workersFlag, *countFlag)
	}

	if *outputFlag == "kafka" {
		if *topicFlag == "" {
			return errors.New("topic must be set when using kafka output")
		}
	}

	if *outputFlag == "batch-grpc-collector" {
		if *tokenFlag == "" {
			return errors.New("token must be set when using batch-grpc-collector output")
		}
	}

	return nil
}
