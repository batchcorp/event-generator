package main

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
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
	OutputNoOp          = "noop"
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

	kingpin.Flag("count", "how many events to generate and send (can be range MIN:MAX or number)").
		Default("1").
		StringVar(&params.StrCount)

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
		Default(OutputGRPCCollector).
		EnumVar(&params.Output, OutputGRPCCollector, OutputKafka, OutputRabbitMQ, OutputNoOp)

	kingpin.Flag("verbose-noop", "Enable verbose noop output").
		BoolVar(&params.VerboseNoOp)

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

	kingpin.Flag("fudge-count", "Number of events that should be fudged (only gRPC + JSON)").
		IntVar(&params.FudgeCount)

	kingpin.Flag("fudge-field", "Field that should be fudged (only gRPC + JSON)").
		StringVar(&params.FudgeField)

	kingpin.Flag("fudge-value", "Value that the field should be updated to (only gRPC + JSON)").
		StringVar(&params.FudgeValue)

	kingpin.Flag("fudge-type", "Type the fudged field will be set to (only gRPC + JSON)").
		Default("string").
		EnumVar(&params.FudgeType, "string", "int", "bool")

	kingpin.Flag("sleep", "sleep for $INPUT milliseconds between batches").
		Default("0").
		IntVar(&params.Sleep)

	kingpin.Flag("continuous", "Continuously generate & send events").
		BoolVar(&params.Continuous)

	kingpin.Flag("continuous-interval", "How long to wait between sending messages (Go time.Duration format; can specify range as MINs:MAXs)").
		Default("1s").
		StringVar(&params.StrContinuousInterval)

	kingpin.Flag("sleep-random", "sleep for $random milliseconds between sends").
		Default("0").
		IntVar(&params.SleepRandom)

	kingpin.Flag("batch-size-random", "randomize the size of batches").
		Default("false").
		BoolVar(&params.BatchSizeRandom)

	kingpin.CommandLine.HelpFlag.Short('h')
	kingpin.Parse()
}

func main() {
	for {
		if err := handleFlags(); err != nil {
			logrus.Fatalf("unable to handle flags: %s", err)
		}

		logrus.Infof("Sending '%d' events in batches of '%d' to '%s' using '%d' workers",
			params.XXXCount, params.BatchSize, params.Address, params.Workers)

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
		case OutputNoOp:
			sendEventsFunc = sendNoOpEvents
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

		if !params.Continuous {
			break
		}

		logrus.Infof("Continuous mode: sleeping for '%s' before next send (CTRL-C to stop)", params.XXXContinuousInterval)
		time.Sleep(params.XXXContinuousInterval)
	}

	logrus.Info("All work completed")
}

func handleCountFlags(params *cli.Params) error {
	if params == nil {
		return errors.New("params cannot be nil")
	}

	if params.StrCount == "" {
		return errors.New("count cannot be empty")
	}

	if !strings.Contains(params.StrCount, ":") {
		count, err := strconv.Atoi(params.StrCount)
		if err != nil {
			return fmt.Errorf("unable to convert count to int: %s", err)
		}

		params.XXXCount = count

		return nil
	}

	// Count is a range
	countRange := strings.Split(params.StrCount, ":")

	if len(countRange) != 2 {
		return fmt.Errorf("unable to parse count range '%s'", params.StrCount)
	}

	min := countRange[0]
	max := countRange[1]

	minCount, err := strconv.Atoi(min)
	if err != nil {
		return fmt.Errorf("unable to convert min count to int: %s", err)
	}

	maxCount, err := strconv.Atoi(max)
	if err != nil {
		return fmt.Errorf("unable to convert max count to int: %s", err)
	}

	params.XXXCountMin = minCount
	params.XXXCountMax = maxCount

	if minCount > maxCount {
		return fmt.Errorf("min count cannot be greater than max count")
	}

	params.XXXCount = rand.Intn(params.XXXCountMax - params.XXXCountMin + 1)

	return nil
}

func handleFlags() error {
	if err := handleCountFlags(params); err != nil {
		return fmt.Errorf("unable to handle count updates: %s", err)
	}

	if params.Workers > params.XXXCount {
		return fmt.Errorf("worker count (%d) cannot exceed count (%d)", params.Workers, params.XXXCount)
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

	// Fudging is only supported with JSON & gRPC
	if params.FudgeCount != 0 {
		if params.Encode != "json" {
			return errors.New("--encode must be 'json' when --fudge is specified")
		}

		if params.Output != OutputGRPCCollector && params.Output != OutputNoOp {
			return fmt.Errorf("--output must be either '%s' or '%s' when --fudge is specified", OutputNoOp, OutputGRPCCollector)
		}
	}

	// If --fudge is set, require that FudgeField and FudgeValue are set
	if params.FudgeCount != 0 && (params.FudgeField == "" || params.FudgeValue == "") {
		return errors.New("--fudge-field and --fudge-value must be set if --fudge is specified")
	}

	// Cannot fudge more than what is requested
	if params.FudgeCount > params.XXXCount {
		return errors.New("fudge value cannot exceed count")
	}

	// If continuous is set, require that --continuous-interval is properly set
	if params.Continuous {
		if err := handleContinuousIntervalFlags(params); err != nil {
			return fmt.Errorf("unable to handle continuous interval: %s", err)
		}
	}

	return nil
}

func handleContinuousIntervalFlags(params *cli.Params) error {
	if params == nil {
		return errors.New("params cannot be nil")
	}

	if !strings.Contains(params.StrContinuousInterval, ":") {
		// Continuous interval is a single time.Duration
		interval, err := time.ParseDuration(params.StrContinuousInterval)
		if err != nil {
			return fmt.Errorf("unable to parse continuous interval: %s", err)
		}

		params.XXXContinuousInterval = interval

		return nil
	}

	// Continuous interval is a range
	intervalRange := strings.Split(params.StrContinuousInterval, ":")

	if len(intervalRange) != 2 {
		return fmt.Errorf("unable to parse continuous interval range '%s'", params.StrContinuousInterval)
	}

	min := intervalRange[0]
	max := intervalRange[1]

	minInterval, err := time.ParseDuration(min)
	if err != nil {
		return fmt.Errorf("unable to parse min continuous interval: %s", err)
	}

	maxInterval, err := time.ParseDuration(max)
	if err != nil {
		return fmt.Errorf("unable to parse max continuous interval: %s", err)
	}

	if minInterval > maxInterval {
		return fmt.Errorf("min continuous interval cannot be greater than max continuous interval")
	}

	params.XXXContinuousIntervalMin = minInterval
	params.XXXContinuousIntervalMax = maxInterval

	// Generate random time.Duration between min and max
	rand.Seed(time.Now().UnixNano())
	params.XXXContinuousInterval = time.Second * time.Duration(rand.Intn(int(maxInterval.Seconds())-int(minInterval.Seconds())+1))

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
