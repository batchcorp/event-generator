package events

import (
	"fmt"

	"github.com/batchcorp/event-generator/cli"
	"github.com/pkg/errors"
)

func GenerateEvents(params *cli.Params) ([]*Event, error) {
	data := make([]*Event, 0)

	switch params.Type {
	case string(TopicTestType):
		data = GenerateTopicTestEvents(params.Count, params.TopicPrefix, params.TopicReplicas, params.TopicPartitions)
	case string(SearchEventType):
		data = GenerateSearchEvents(params.Count)
	case string(BillingEventType):
		data = GenerateBillingEvents(params.Count)
	case string(MonitoringEventType):
		return nil, errors.New("not implemented")
	case string(AuditEventType):
		return nil, errors.New("not implemented")
	case string(ForgotPasswordType):
		return nil, errors.New("not implemented")
	default:
		return nil, fmt.Errorf("unknown event type '%s'", params.Type)
	}

	return data, nil
}
