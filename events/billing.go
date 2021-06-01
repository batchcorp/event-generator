package events

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	uuid "github.com/satori/go.uuid"
)

var (
	products  []*BillingProduct
	states    = []string{"NEW", "CANCELLED", "PROCESSED", "FAILED"}
	usernames = []string{
		"unlucky", "foobar", "sniperx", "fpsdriver", "batboi", "spidermn22",
		"solucky", "reginaldo15", "hackerman", "zerocool", "acidburn", "lordnikon",
		"drdoom", "cerealkiller",
	}
)

func init() {
	for i := 0; i < 100; i++ {
		products = append(products, &BillingProduct{
			ID:       uuid.NewV4().String(),
			Name:     gofakeit.Vegetable(),
			Quantity: gofakeit.RandomInt([]int{1, 2, 3, 4, 5}),
			Price:    gofakeit.Price(1.00, 15.00),
		})
	}
}

type Billing struct {
	OrderID    string            `json:"order_id"`
	OrderState string            `json:"order_state"`
	CustomerID string            `json:"customer_id"`
	OrderDate  int64             `json:"order_date"`
	Products   []*BillingProduct `json:"products"`
}

type BillingProduct struct {
	ID       string  `json:"id"`
	Name     string  `json:"name"`
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
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

	return events
}

func (b *Billing) Fill() {
	b.OrderID = uuid.NewV4().String()
	b.OrderState = gofakeit.RandomString(states)
	b.CustomerID = fmt.Sprintf("%s-%d", usernames[rand.Intn(len(usernames)-1)+1], gofakeit.RandomInt([]int{1, 2, 3, 123}))
	b.OrderDate = gofakeit.DateRange(time.Now(), time.Now()).Unix()
	b.Products = randomProducts()
}

func randomProducts() []*BillingProduct {
	rand.Seed(time.Now().UnixNano())
	numProducts := rand.Intn(10-1) + 1

	localProducts := make([]*BillingProduct, numProducts)

	for i := 0; i < numProducts; i++ {
		localProducts[i] = products[rand.Intn(100-1)+1]
	}

	return localProducts
}
