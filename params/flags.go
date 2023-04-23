package params

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/batchcorp/event-generator/events"
	"github.com/batchcorp/event-generator/params/types"
)

const (
	OutputGRPCCollector = "batch-grpc-collector"
	OutputKafka         = "kafka"
	OutputRabbitMQ      = "rabbitmq"
	OutputPulsar        = "pulsar"
	OutputNoOp          = "noop"
)

func parseKingpinFlags(p *types.Params) {
	kingpin.Flag("type", "type of event(s) to generate (options: billing, search, products, posts, users, weather, coins)").
		Short('t').
		Required().
		EnumVar(&p.Type, events.BillingEventType, events.SearchEventType,
			events.ProductsEventType, events.UsersEventType, events.PostsEventType,
			events.WeatherEventType, events.CoinsEventType, events.BillingV2EventType)

	kingpin.Flag("topic-prefix", "What prefix to use for new topics (only used if 'type' is topic_test)").
		Default("test").
		StringVar(&p.TopicPrefix)

	kingpin.Flag("topic-replicas", "TopicReplicaCount count configuration for topic (only used if 'type' is topic_test)").
		Default("1").
		IntVar(&p.TopicReplicas)

	kingpin.Flag("topic-partitions", "TopicPartitionCount count configuration for topic (only used if 'type' is topic_test)").
		Default("1").
		IntVar(&p.TopicPartitions)

	kingpin.Flag("token", "Batch token").
		StringVar(&p.Token)

	kingpin.Flag("count", "how many events to generate and send (can be range MIN:MAX or number)").
		Short('c').
		Default("1").
		StringVar(&p.StrCount)

	kingpin.Flag("encode", "encode the event as JSON (default) or protobuf").
		Short('e').
		Default("json").
		EnumVar(&p.Encode, "json", "protobuf")

	kingpin.Flag("batch-size", "how many events to send in a single batch (can be range MIN:MAX or number)").
		Short('b').
		Default("100").
		StringVar(&p.StrBatchSize)

	kingpin.Flag("workers", "how many workers to use").
		Short('w').
		Default("1").
		IntVar(&p.Workers)

	kingpin.Flag("disable-tls", "disable tls").
		Default("false").
		BoolVar(&p.DisableTLS)

	kingpin.Flag("address", "where to send events (used for all outputs; pulsar address should be in form of 'pulsar://host:port')").
		Default("grpc-collector.dev.streamdal.com:9000").
		StringVar(&p.Address)

	kingpin.Flag("output", "what kind of destination is this").
		Short('o').
		Default(OutputGRPCCollector).
		EnumVar(&p.Output, OutputGRPCCollector, OutputKafka, OutputRabbitMQ, OutputPulsar, OutputNoOp)

	kingpin.Flag("verbose", "Enable verbose output").
		BoolVar(&p.Verbose)

	kingpin.Flag("topic", "topic to write events to (kafka and pulsar only!)").
		StringVar(&p.Topic)

	kingpin.Flag("rabbit-exchange", "which exchange to write to").
		StringVar(&p.RabbitExchange)

	kingpin.Flag("rabbit-routing-key", "what routing key to use when writing data").
		StringVar(&p.RabbitRoutingKey)

	kingpin.Flag("rabbit-declare-exchange", "whether to declare exchange").
		BoolVar(&p.RabbitDeclareExchange)

	kingpin.Flag("rabbit-durable-exchange", "whether the exchange should be durable").
		BoolVar(&p.RabbitDeclareExchange)

	kingpin.Flag("fudge-count", "Number of events that should be fudged (can be range MIN:MAX or number; only gRPC w/ JSON)").
		StringVar(&p.StrFudgeCount)

	kingpin.Flag("fudge-field", "Field that should be fudged (format: obj1.obj2.obj3; only gRPC + JSON)").
		StringVar(&p.FudgeField)

	kingpin.Flag("fudge-value", "Value that the field should be updated to (only gRPC + JSON)").
		StringVar(&p.FudgeValue)

	kingpin.Flag("fudge-type", "Type the fudged field will be set to (options: string, int, bool; only gRPC + JSON)").
		Default("string").
		EnumVar(&p.FudgeType, "string", "int", "bool")

	kingpin.Flag("sleep", "sleep for $INPUT milliseconds between batches (can be MIN:MAX or number)").
		Short('s').
		Default("0").
		StringVar(&p.StrSleep)

	kingpin.Flag("continuous", "Send forever at given interval (Golang time.Duration format; can specify range as MIN:MAX)").
		StringVar(&p.StrContinuous)

	kingpin.Flag("dead-letter", "Force all messages to dead letter").
		BoolVar(&p.ForceDeadLetter)

	kingpin.Flag("async-producer", "Produce messages async (pulsar-only)").
		BoolVar(&p.PulsarAsyncProducer)

	kingpin.CommandLine.HelpFlag.Short('h')
	kingpin.Parse()
}

func HandleParams(p *types.Params) error {
	parseKingpinFlags(p)

	// Parse count flag
	count, err := GetIntegerRange(p.StrCount)
	if err != nil {
		return fmt.Errorf("unable to get count: %s", err)
	}

	p.XXXCount = count.Value
	p.XXXCountMin = count.Min
	p.XXXCountMax = count.Max

	if p.Workers > p.XXXCount {
		return fmt.Errorf("worker count (%d) cannot exceed count (%d)", p.Workers, p.XXXCount)
	}

	// Parse fudge count flag
	if p.StrFudgeCount != "" {
		fudgeCount, err := GetIntegerRange(p.StrFudgeCount)
		if err != nil {
			return fmt.Errorf("unable to get fudge count: %s", err)
		}

		p.XXXFudgeCount = fudgeCount.Value
		p.XXXFudgeCountMin = fudgeCount.Min
		p.XXXFudgeCountMax = fudgeCount.Max

		// Fudging is only supported with JSON & gRPC
		if p.Encode != "json" {
			return errors.New("--encode must be 'json' when --fudge is specified")
		}

		if p.Output != OutputGRPCCollector && p.Output != OutputNoOp && p.Output != OutputRabbitMQ {
			opts := strings.Join([]string{OutputNoOp, OutputGRPCCollector, OutputRabbitMQ}, ", ")
			return fmt.Errorf("--output must be one of %s when --fudge-count is specified", opts)
		}

		// If fudge is larger than count, update fudged count to match count
		if p.XXXFudgeCount > p.XXXCount {
			p.XXXFudgeCount = p.XXXCount
		}

		// If --fudge-count is set, require that FudgeField and FudgeValue are set
		if p.FudgeField == "" || p.FudgeValue == "" || p.FudgeType == "" {
			return errors.New("--fudge-field, --fudge-value and --fudge-type must be set if --fudge-count is specified")
		}
	}

	// Parse continuous flag(s)
	if p.StrContinuous != "" {
		continuousInterval, err := GetIntervalRange(p.StrContinuous)
		if err != nil {
			return fmt.Errorf("unable to get continuous interval: %s", err)
		}

		p.XXXContinuous = continuousInterval.Value
		p.XXXContinuousMin = continuousInterval.Min
		p.XXXContinuousMax = continuousInterval.Max

		if p.XXXContinuousMin > p.XXXContinuousMax {
			return errors.New("min continuous interval cannot be greater than max continuous interval")
		}
	}

	// Parse batch size flag
	if p.StrBatchSize != "" {
		batchSize, err := GetIntegerRange(p.StrBatchSize)
		if err != nil {
			return fmt.Errorf("unable to get batch size: %s", err)
		}

		p.XXXBatchSize = batchSize.Value
		p.XXXBatchSizeMin = batchSize.Min
		p.XXXBatchSizeMax = batchSize.Max

		if p.XXXBatchSizeMin > p.XXXBatchSizeMax {
			return errors.New("min batch size cannot be greater than max batch size")
		}

		if p.XXXBatchSize == 0 {
			return errors.New("batch size cannot must be >0")
		}
	}

	// Parse sleep interval flags
	if p.StrSleep != "" {
		sleepInterval, err := GetIntervalRange(p.StrSleep)
		if err != nil {
			return fmt.Errorf("unable to get sleep interval: %s", err)
		}

		p.XXXSleep = sleepInterval.Value
		p.XXXSleepMin = sleepInterval.Min
		p.XXXSleepMax = sleepInterval.Max

		if p.XXXSleepMin > p.XXXSleepMax {
			return errors.New("min sleep interval cannot be greater than max sleep interval")
		}
	}

	if p.Output == OutputKafka {
		if p.Topic == "" {
			return errors.New("topic must be set when using kafka output")
		}
	}

	if p.Output == OutputGRPCCollector {
		if p.Token == "" {
			return errors.New("token must be set when using batch-grpc-collector output")
		}
	}

	if p.Type == OutputRabbitMQ {
		if p.RabbitExchange == "" {
			return errors.New("--rabbit-exchange cannot be empty with rabbit output")
		}

		if p.RabbitRoutingKey == "" {
			return errors.New("--rabbit-routing-key cannot be empty with rabbit output")
		}
	}

	if p.PulsarAsyncProducer && p.Output != OutputPulsar {
		return errors.New("--async-producer can only be used with pulsar output")
	}

	return nil
}
