package events

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	uuid "github.com/satori/go.uuid"

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

type Search struct {
	Query      string `json:"query"`
	Collection string `json:"collection"`
	UserID     string `json:"user_id"`
	TeamID     string `json:"team_id"`
	Domain     string `json:"domain"`
	Timestamp  int64  `json:"timestamp"`
}

func GenerateSearchEvents(count int) []*Event {
	events := make([]*Event, 0)

	for i := 0; i < count; i++ {
		event := &Search{}
		event.Fill()

		events = append(events, &Event{
			Type:          SearchEventType,
			TimestampNano: time.Now().UTC().UnixNano(),
			RequestID:     uuid.NewV4().String(),
			Source:        gofakeit.AppName(),
			Search:        event,
		})
	}

	return events
}

func (s *Search) Fill() {
	s.Query = query()
	s.Collection = gofakeit.Noun()
	s.UserID = generator.RandomUserID()
	s.TeamID = generator.RandomTeamID()
	s.Timestamp = time.Now().UTC().UnixNano()
	s.Domain = gofakeit.DomainName()
}

func query() string {
	rand.Seed(time.Now().UnixNano())
	return queries[rand.Intn(len(queries)-1)]
}
