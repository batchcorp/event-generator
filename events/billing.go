package events

import (
	"time"

	"github.com/brianvoe/gofakeit/v6"
	uuid "github.com/satori/go.uuid"
)

type Billing struct {
	OrderID    string            `json:"order_id"`
	OrderState string            `json:"order_state"`
	CustomerID string            `json:"customer_id"`
	OrderDate  int               `json:"order_date"`
	Products   []*BillingProduct `json:"products"`
}

type BillingProduct struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Quantity int    `json:"quantity"`
	Price    int    `json:"price"`
}

func GenerateBillingEvents(count int) []*Event {
	events := make([]*Event, 0)

	for i := 0; i < count; i++ {
		event := &Billing{}
		event.Fill()

		events = append(events, &Event{
			Type:          BillingEventType,
			TimestampNano: time.Now().UTC().UnixNano(),
			RequestID:     uuid.NewV4().String(),
			Source:        gofakeit.AppName(),
			Billing:       event,
		})
	}

	return nil
}

func (b *Billing) Fill() {
}
