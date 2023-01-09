package events

import (
	"fmt"

	"github.com/batchcorp/schemas/build/go/events/fakes"

	"github.com/batchcorp/event-generator/params/types"
)

func GenerateEvents(p *types.Params) (chan *fakes.Event, error) {
	generateChan := make(chan *fakes.Event)

	switch p.Type {
	case SearchEventType:
		go GenerateSearchEvents(p, generateChan)
	case BillingEventType:
		go GenerateBillingEvents(p, generateChan)

	default:
		return nil, fmt.Errorf("unknown event type '%s'", p.Type)
	}

	return generateChan, nil
}
