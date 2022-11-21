package events

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/batchcorp/schemas/build/go/events/fakes"
	"github.com/brianvoe/gofakeit/v6"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/event-generator/cli"
)

var (
	products  []*fakes.BillingProduct
	states    = []string{"NEW", "CANCELLED", "PROCESSED", "FAILED"}
	usernames = []string{
		"unlucky", "foobar", "sniperx", "fpsdriver", "batboi", "spidermn22",
		"solucky", "reginaldo15", "hackerman", "zerocool", "acidburn", "lordnikon",
		"drdoom", "cerealkiller",
	}
)

func init() {
	// Pre-create some products
	for i := 0; i < 100; i++ {
		products = append(products, &fakes.BillingProduct{
			Id:       uuid.NewV4().String(),
			Name:     gofakeit.Vegetable(),
			Quantity: int32(gofakeit.RandomInt([]int{1, 2, 3, 4, 5})),
			Price:    float32(gofakeit.Price(1.00, 15.00)),
		})
	}
}

func GenerateBillingEvents(params *cli.Params, generateChan chan *fakes.Event) {
	defer close(generateChan)

	for i := 0; i < params.Count; i++ {
		generateChan <- &fakes.Event{
			Type:          fakes.EventType_EVENT_TYPE_BILLING,
			TimestampNano: time.Now().UTC().UnixNano(),
			RequestId:     uuid.NewV4().String(),
			Source:        gofakeit.AppName(),
			Event: &fakes.Event_Billing{
				Billing: newBillingEvent(),
			},
		}
	}
}

func newBillingEvent() *fakes.Billing {
	return &fakes.Billing{
		OrderId:    uuid.NewV4().String(),
		OrderState: gofakeit.RandomString(states),
		CustomerId: fmt.Sprintf("%s-%d", usernames[rand.Intn(len(usernames)-1)+1], gofakeit.RandomInt([]int{1, 2, 3, 123})),
		OrderDate:  gofakeit.DateRange(time.Now(), time.Now()).Unix(),
		Products:   randomProducts(),
	}
}

func randomProducts() []*fakes.BillingProduct {
	rand.Seed(time.Now().UnixNano())
	numProducts := rand.Intn(10-1) + 1

	localProducts := make([]*fakes.BillingProduct, numProducts)

	for i := 0; i < numProducts; i++ {
		localProducts[i] = products[rand.Intn(100-1)+1]
	}

	return localProducts
}
