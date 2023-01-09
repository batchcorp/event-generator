package events

import (
	"time"

	"github.com/brianvoe/gofakeit/v6"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/schemas/build/go/events/fakes"

	"github.com/batchcorp/event-generator/params/types"
)

func GenerateWeatherEvents(p *types.Params, generateChan chan *fakes.Event) {
	defer close(generateChan)

	for i := 0; i < p.XXXCount; i++ {
		generateChan <- &fakes.Event{
			Type:          fakes.EventType_EVENT_TYPE_WEATHER,
			TimestampNano: time.Now().UTC().UnixNano(),
			RequestId:     uuid.NewV4().String(),
			Source:        gofakeit.AppName(),
			Event: &fakes.Event_Weather{
				Weather: newWeatherEvent(),
			},
		}
	}
}

func newWeatherEvent() *fakes.Weather {
	return &fakes.Weather{
		Coordinates: &fakes.WeatherCoordinates{
			Latitude:  float32(gofakeit.Latitude()),
			Longitude: float32(gofakeit.Longitude()),
		},
		Overview: &fakes.WeatherOverview{
			Id:          gofakeit.UUID(),
			Description: gofakeit.SentenceSimple(),
			IconUrl:     "https://img.streamdal.com/weather/" + gofakeit.UUID() + "-icon.png",
		},
		Base: &fakes.WeatherBase{
			StationId:      gofakeit.UUID(),
			StationName:    gofakeit.Company(),
			StationCountry: gofakeit.Country(),
		},
		Info: &fakes.WeatherInfo{
			TemperatureC: float32(gofakeit.Number(-50, 50)),
			FeelsLikeC:   float32(gofakeit.Number(-50, 50)),
			TempMin:      float32(gofakeit.Number(-50, 50)),
			TempMax:      float32(gofakeit.Number(-50, 50)),
			Pressure:     int32(gofakeit.Number(0, 100)),
			Humidity:     int32(gofakeit.Number(0, 100)),
			SeaLevel:     int32(gofakeit.Number(0, 100)),
			GroundLevel:  int32(gofakeit.Number(0, 100)),
		},
		Visibility: int32(gofakeit.Number(0, 100)),
		Wind: &fakes.WeatherWind{
			Speed: float32(gofakeit.Number(0, 100)),
			Deg:   int32(gofakeit.Number(0, 360)),
			Gust:  float32(gofakeit.Number(0, 100)),
		},
		Rain: &fakes.WeatherRain{
			OneHour: float32(gofakeit.Number(0, 100)),
		},
		Clouds: &fakes.WeatherClouds{
			All: int32(gofakeit.Number(0, 100)),
		},
		Timezone:        gofakeit.TimeZone(),
		UnixTimeNanoUtc: time.Now().Add(time.Minute * time.Duration(gofakeit.Number(-60, 60))).UTC().UnixNano(),
		System: &fakes.WeatherSystem{
			Type:    int32(gofakeit.Number(1, 100)),
			Id:      int32(gofakeit.Number(1, 100)),
			Message: gofakeit.Float32Range(0, 1),
			Country: gofakeit.Country(),
			Sunrise: int32(gofakeit.Number(4, 12)),
			Sunset:  int32(gofakeit.Number(15, 22)),
		},
	}
}
