package cli

import (
	"time"
)

// This is in a separate package to avoid cyclic import errors.

type Params struct {
	Type                  string
	TopicPrefix           string
	TopicReplicas         int
	TopicPartitions       int
	Token                 string
	StrCount              string
	BatchSize             int
	Workers               int
	DisableTLS            bool
	Address               string
	Output                string
	Topic                 string
	Sleep                 int
	SleepRandom           int
	BatchSizeRandom       bool
	Fudge                 int
	FudgeField            string
	FudgeValue            string
	FudgeType             string
	Encode                string
	RabbitExchange        string
	RabbitRoutingKey      string
	RabbitDeclareExchange bool
	RabbitDurableExchange bool
	VerboseNoOp           bool

	Continuous            bool
	StrContinuousInterval string

	XXXCount    int
	XXXCountMin int
	XXXCountMax int

	XXXContinuousInterval    time.Duration
	XXXContinuousIntervalMin time.Duration
	XXXContinuousIntervalMax time.Duration
}
