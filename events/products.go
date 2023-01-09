package events

import (
	"time"

	"github.com/brianvoe/gofakeit/v6"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/schemas/build/go/events/fakes"

	"github.com/batchcorp/event-generator/params/types"
)

func GenerateProductEvents(p *types.Params, generateChan chan *fakes.Event) {
	defer close(generateChan)

	for i := 0; i < p.XXXCount; i++ {
		generateChan <- &fakes.Event{
			Type:          fakes.EventType_EVENT_TYPE_PRODUCTS,
			TimestampNano: time.Now().UTC().UnixNano(),
			RequestId:     uuid.NewV4().String(),
			Source:        gofakeit.AppName(),
			Event: &fakes.Event_Product{
				Product: newProductEvent(),
			},
		}
	}
}

func newProductEvent() *fakes.Product {
	product := randomFoodProduct()

	return &fakes.Product{
		Id:                 gofakeit.UUID(),
		Title:              product.Title,
		Description:        product.Description,
		Price:              int64(gofakeit.Price(0, 100)),
		DiscountPercentage: float32(gofakeit.Price(1, 5)),
		Rating:             float32(gofakeit.RandomInt([]int{1, 2, 3, 4, 5})),
		Stock:              int64(gofakeit.Number(1, 1000)),
		Brand:              gofakeit.Company(),
		Category:           product.Category,
		ThumbnailUrl:       "https://img.streamdal.com/thumbnail/" + gofakeit.UUID() + ".png",
		ImageUrls:          product.ImageURLs,
		Tags:               product.Tags,
	}
}

type foodProduct struct {
	Title       string
	Description string
	Category    string
	Tags        []string
	ImageURLs   []string
}

func randomFoodProduct() *foodProduct {
	randProducts := map[string]func() string{
		"fruit":     gofakeit.Fruit,
		"vegetable": gofakeit.Vegetable,
		"beer":      gofakeit.BeerName,
		"dessert":   gofakeit.Dessert,
		"lunch":     gofakeit.Lunch,
		"dinner":    gofakeit.Dinner,
		"snack":     gofakeit.Snack,
		"breakfast": gofakeit.Breakfast,
	}

	randKey := gofakeit.RandomString([]string{"fruit", "vegetable", "beer", "dessert", "lunch", "dinner", "snack", "breakfast"})
	title := randProducts[randKey]()
	category := randKey
	description := gofakeit.Sentence(10)

	tags := make([]string, 0)

	for i := 0; i < gofakeit.Number(0, 5); i++ {
		tags = append(tags, gofakeit.AdjectiveDescriptive())
	}

	imageURLs := make([]string, 0)

	for i := 0; i < gofakeit.Number(0, 5); i++ {
		imageURLs = append(imageURLs, "https://img.streamdal.com/img/"+gofakeit.UUID()+".png")
	}

	return &foodProduct{
		Title:       title,
		Description: description,
		Category:    category,
		Tags:        tags,
		ImageURLs:   imageURLs,
	}
}
