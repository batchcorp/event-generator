package types

import (
	"time"
)

type Params struct {
	Type                  string
	TopicPrefix           string
	TopicReplicas         int
	TopicPartitions       int
	Token                 string
	Workers               int
	DisableTLS            bool
	Address               string
	Output                string
	Topic                 string
	FudgeField            string
	FudgeValue            string
	FudgeType             string
	Encode                string
	RabbitExchange        string
	RabbitRoutingKey      string
	RabbitDeclareExchange bool
	RabbitDurableExchange bool
	Verbose               bool

	StrCount    string
	XXXCount    int
	XXXCountMin int
	XXXCountMax int

	StrFudgeCount    string
	XXXFudgeCount    int
	XXXFudgeCountMin int
	XXXFudgeCountMax int

	StrSleep    string
	XXXSleep    time.Duration
	XXXSleepMin time.Duration
	XXXSleepMax time.Duration

	StrContinuous    string
	XXXContinuous    time.Duration
	XXXContinuousMin time.Duration
	XXXContinuousMax time.Duration

	StrBatchSize    string
	XXXBatchSize    int
	XXXBatchSizeMin int
	XXXBatchSizeMax int
}

type IntegerRange struct {
	Value int
	Min   int
	Max   int
}

type IntervalRange struct {
	Value time.Duration
	Min   time.Duration
	Max   time.Duration
}
