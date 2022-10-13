package cli

// This is in a separate package to avoid cyclic import errors.

type Params struct {
	Type                  string
	TopicPrefix           string
	TopicReplicas         int
	TopicPartitions       int
	Token                 string
	Count                 int
	BatchSize             int
	Workers               int
	DisableTLS            bool
	Address               string
	Output                string
	Topic                 string
	Sleep                 int
	Randomize             bool
	Encode                string
	RabbitExchange        string
	RabbitRoutingKey      string
	RabbitDeclareExchange bool
}
