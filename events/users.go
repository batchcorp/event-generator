package events

import (
	"strings"
	"time"

	"github.com/batchcorp/schemas/build/go/events/fakes"
	"github.com/brianvoe/gofakeit/v6"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/event-generator/params/types"
)

func GenerateUserEvents(p *types.Params, generateChan chan *fakes.Event) {
	defer close(generateChan)

	for i := 0; i < p.XXXCount; i++ {
		generateChan <- &fakes.Event{
			Type:          fakes.EventType_EVENT_TYPE_USERS,
			TimestampNano: time.Now().UTC().UnixNano(),
			RequestId:     uuid.NewV4().String(),
			Source:        gofakeit.AppName(),
			Event: &fakes.Event_User{
				User: newUser(),
			},
		}
	}
}

func newUser() *fakes.User {
	cc := gofakeit.CreditCard()

	return &fakes.User{
		Id:            gofakeit.UUID(),
		FirstName:     gofakeit.FirstName(),
		LastName:      gofakeit.LastName(),
		MaidenName:    gofakeit.LastName(),
		Age:           int32(gofakeit.Number(18, 100)),
		Gender:        gofakeit.Gender(),
		Email:         gofakeit.Email(),
		Phone:         gofakeit.Phone(),
		Username:      gofakeit.Username(),
		BirthDate:     gofakeit.DateRange(time.Now().AddDate(-100, 0, 0), time.Now()).Format("2006-01-02"),
		ProfileImgUrl: "https://img.streamdal.com/profile/" + gofakeit.UUID() + ".png",
		BloodGroup:    gofakeit.RandomString([]string{"A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"}),
		EyeColor:      gofakeit.SafeColor(),
		Hair: &fakes.UserHair{
			Color: gofakeit.Color(),
			Style: gofakeit.AdjectiveDemonstrative(),
		},
		Domain:    gofakeit.DomainName(),
		IpAddress: gofakeit.IPv4Address(),
		Address: &fakes.UserAddress{
			Street:      gofakeit.Street(),
			City:        gofakeit.City(),
			State:       gofakeit.State(),
			Zip:         gofakeit.Zip(),
			CountryCode: gofakeit.CountryAbr(),
			Coordinates: &fakes.UserCoordinates{
				Latitude:  float32(gofakeit.Latitude()),
				Longitude: float32(gofakeit.Longitude()),
			},
		},
		MacAddress: gofakeit.MacAddress(),
		University: strings.ToTitle(gofakeit.Word()) + " University",
		Bank: &fakes.UserBank{
			CardExpireMonth: int32(gofakeit.Number(2022, 2030)),
			CardExpireYear:  int32(gofakeit.Number(1, 12)),
			CardNumber:      int64(gofakeit.Number(1000000000000000, 9999999999999999)),
			CardType:        cc.Type,
			Currency:        gofakeit.CurrencyShort(),
			Iban:            gofakeit.AchAccount(),
		},
		Company: nil,
	}
}
