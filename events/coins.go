package events

import (
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/schemas/build/go/events/fakes"

	"github.com/batchcorp/event-generator/params/types"
)

func GenerateCoinEvents(p *types.Params, generateChan chan *fakes.Event) {
	defer close(generateChan)

	for i := 0; i < p.XXXCount; i++ {
		generateChan <- &fakes.Event{
			Type:          fakes.EventType_EVENT_TYPE_COINS,
			TimestampNano: time.Now().UTC().UnixNano(),
			RequestId:     uuid.NewV4().String(),
			Source:        gofakeit.AppName(),
			Event: &fakes.Event_Coin{
				Coin: newCoinEvent(),
			},
		}
	}
}

func newCoinEvent() *fakes.Coin {
	coin := randomCoin()

	return &fakes.Coin{
		Type:              fakes.CoinType(gofakeit.Number(1, 13)),
		IconUrl:           "https://img.streamdal.com/coin/" + strings.ToLower(coin.Name) + ".png",
		Name:              coin.Name,
		Symbol:            coin.Symbol,
		Rank:              int64(gofakeit.Number(0, 10)),
		Price:             int64(gofakeit.Number(1, 1000)),
		Volume:            gofakeit.Float32Range(0, 1000000),
		MarketCap:         gofakeit.Float32Range(0, 100),
		AvailableSupply:   int64(gofakeit.Number(100, 100000)),
		TotalSupply:       int64(gofakeit.Number(100000, 10000000)),
		PercentChange_1H:  gofakeit.Float32Range(0, 5),
		PercentChange_24H: gofakeit.Float32Range(0, 5),
		PercentChange_7D:  gofakeit.Float32Range(0, 5),
		Description:       gofakeit.SentenceSimple(),
		WebsiteUrl:        gofakeit.DomainName(),
		TwitterUrl:        "twitter.com/" + gofakeit.Username(),
		LastUpdated:       gofakeit.DateRange(time.Now().AddDate(0, 0, -1), time.Now()).UnixNano(),
	}
}

type Coin struct {
	Name   string
	Symbol string
}

func randomCoin() *Coin {
	coins := []*Coin{
		{
			Name:   "Bitcoin",
			Symbol: "BTC",
		},
		{
			Name:   "Ethereum",
			Symbol: "ETH",
		},
		{
			Name:   "Ripple",
			Symbol: "XRP",
		},
		{
			Name:   "Bitcoin Cash",
			Symbol: "BCH",
		},
		{
			Name:   "EOS",
			Symbol: "EOS",
		},
		{
			Name:   "Litecoin",
			Symbol: "LTC",
		},
		{
			Name:   "Stellar",
			Symbol: "XLM",
		},
		{
			Name:   "Cardano",
			Symbol: "ADA",
		},
		{
			Name:   "TRON",
			Symbol: "TRX",
		},
		{
			Name:   "IOTA",
			Symbol: "MIOTA",
		},
		{
			Name:   "NEO",
			Symbol: "NEO",
		},
		{
			Name:   "Dash",
			Symbol: "DASH",
		},
		{
			Name:   "Monero",
			Symbol: "XMR",
		},
	}

	return coins[gofakeit.Number(0, len(coins)-1)]
}
