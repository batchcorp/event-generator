package events

import (
	"fmt"

	"github.com/batchcorp/schemas/build/go/events/fakes"
	"github.com/pkg/errors"

	"github.com/batchcorp/event-generator/cli"
)

func GenerateEvents(params *cli.Params) (chan *fakes.Event, error) {
	generateChan := make(chan *fakes.Event, 1000)

	switch params.Type {
	case string(TopicTestType):
		//data = GenerateTopicTestEvents(params.Count, params.TopicPrefix, params.TopicReplicas, params.TopicPartitions)
		return nil, errors.New("not implemented")
	case string(SearchEventType):
		go GenerateSearchEvents(params.Count, generateChan)
	case string(BillingEventType):
		go GenerateBillingEvents(params.Count, generateChan)
	case string(MonitoringEventType):
		return nil, errors.New("not implemented")
	case string(AuditEventType):
		return nil, errors.New("not implemented")
	case string(ForgotPasswordType):
		return nil, errors.New("not implemented")
	default:
		return nil, fmt.Errorf("unknown event type '%s'", params.Type)
	}

	return generateChan, nil
}
