package events

import (
	"fmt"

	"github.com/pkg/errors"
)

func GenerateEvents(eventType string, count int) ([]*Event, error) {
	data := make([]*Event, 0)

	switch eventType {
	case string(SearchEventType):
		data = GenerateSearchEvents(count)
	case string(BillingEventType):
		data = GenerateBillingEvents(count)
	case string(MonitoringEventType):
		return nil, errors.New("not implemented")
	case string(AuditEventType):
		return nil, errors.New("not implemented")
	case string(ForgotPasswordType):
		return nil, errors.New("not implemented")
	default:
		return nil, fmt.Errorf("unknown event type '%s'", eventType)
	}

	return data, nil
}
