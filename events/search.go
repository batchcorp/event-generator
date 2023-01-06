package events

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/schemas/build/go/events/fakes"

	"github.com/batchcorp/event-generator/cli"
	"github.com/batchcorp/event-generator/generator"
)

var (
	queries = []string{
		fmt.Sprintf("SELECT * FROM %s WHERE %s  = '%s'",
			gofakeit.Noun(), gofakeit.Noun(), gofakeit.BeerName()),

		fmt.Sprintf("SELECT account FROM accounts WHERE user_id = '%s'",
			generator.RandomUserID()),

		fmt.Sprintf("SELECT * FROM records WHERE first_name = %s AND country = '%s' GROUP BY age",
			gofakeit.FirstName(), gofakeit.Country()),

		fmt.Sprintf("SELECT * FROM beers WHERE beer_style = '%s' LIMIT %d",
			gofakeit.BeerStyle(), gofakeit.Number(1, 10)),

		fmt.Sprintf("DELETE FROM hits WHERE source_ip = '%s'",
			gofakeit.IPv4Address()),

		fmt.Sprintf("UPDATE favorites SET color = '%s' WHERE user_id = '%s'",
			gofakeit.Color(), generator.RandomUserID()),

		fmt.Sprintf("UPDATE username SET username = '%s' WHERE user_id = '%s' and team_id = '%s'",
			gofakeit.Username(), generator.RandomUserID(), generator.RandomTeamID()),

		fmt.Sprintf("TRUNCATE table %s", gofakeit.Noun()),
	}
)

func GenerateSearchEvents(params *cli.Params, generateChan chan *fakes.Event) {
	defer close(generateChan)

	for i := 0; i < params.XXXCount; i++ {
		generateChan <- &fakes.Event{
			Type:          fakes.EventType_EVENT_TYPE_SEARCH,
			TimestampNano: time.Now().UTC().UnixNano(),
			RequestId:     uuid.NewV4().String(),
			Source:        gofakeit.AppName(),
			Event: &fakes.Event_Search{
				Search: newSearchEvent(),
			},
		}
	}
}

func newSearchEvent() *fakes.Search {
	return &fakes.Search{
		Query:      query(),
		Collection: gofakeit.Noun(),
		UserId:     generator.RandomUserID(),
		TeamId:     generator.RandomTeamID(),
		Domain:     gofakeit.DomainName(),
		Timestamp:  time.Now().UTC().UnixNano(),
	}
}

func query() string {
	rand.Seed(time.Now().UnixNano())
	return queries[rand.Intn(len(queries)-1)]
}
