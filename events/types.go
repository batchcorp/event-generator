package events

const (
	BillingEventType  EventType = "billing"
	SearchEventType   EventType = "search"
	CoinsEventType    EventType = "coins"
	ProductsEventType EventType = "products"
	UsersEventType    EventType = "users"
	PostsEventType    EventType = "posts"
	WeatherEventType  EventType = "weather"
)

type EventType string
