package events

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/batchcorp/schemas/build/go/events/fakes"
	"github.com/brianvoe/gofakeit/v6"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/event-generator/params/types"
)

var (
	productsV2  []*fakes.BillingProductV2
	statesV2    = []string{"NEW", "CANCELLED", "PROCESSED", "FAILED"}
	usernamesV2 = []string{
		"unlucky", "foobar", "sniperx", "fpsdriver", "batboi", "spidermn22",
		"solucky", "reginaldo15", "hackerman", "zerocool", "acidburn", "lordnikon",
		"drdoom", "cerealkiller",
	}
)

func init() {
	// Pre-create some productsV2
	for i := 0; i < 100; i++ {
		productsV2 = append(productsV2, &fakes.BillingProductV2{
			Id:       uuid.NewV4().String(),
			Name:     gofakeit.Vegetable(),
			Quantity: int32(gofakeit.RandomInt([]int{1, 2, 3, 4, 5})),
			Price:    float32(gofakeit.Price(1.00, 15.00)),
		})
	}
}

func GenerateBillingV2Events(p *types.Params, generateChan chan *fakes.Event) {
	defer close(generateChan)

	for i := 0; i < p.XXXCount; i++ {
		generateChan <- &fakes.Event{
			Type:          fakes.EventType_EVENT_TYPE_BILLING,
			TimestampNano: time.Now().UTC().UnixNano(),
			RequestId:     uuid.NewV4().String(),
			Source:        gofakeit.AppName(),
			Event: &fakes.Event_BillingV2{
				BillingV2: newBillingV2Event(),
			},
		}
	}
}

func newBillingV2Event() *fakes.BillingV2 {
	return &fakes.BillingV2{
		OrderId:    uuid.NewV4().String(),
		OrderState: gofakeit.RandomString(statesV2),
		CustomerId: fmt.Sprintf("%s-%d", usernamesV2[rand.Intn(len(usernamesV2)-1)+1], gofakeit.RandomInt([]int{1, 2, 3, 123})),
		OrderDate:  gofakeit.DateRange(time.Now(), time.Now()).Unix(),
		Product:    randomProduct(),
	}
}

func randomProduct() *fakes.BillingProductV2 {
	rand.Seed(time.Now().UnixNano())
	return productsV2[rand.Intn(100-1)+1]
}
