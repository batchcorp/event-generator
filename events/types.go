package events

const (
	BillingEventType    EventType = "billing"
	MonitoringEventType EventType = "monitoring"
	AuditEventType      EventType = "audit"
	SearchEventType     EventType = "search"
	ForgotPasswordType  EventType = "forgot_password"
)

type EventType string

type Event struct {
	Type          EventType `json:"type"`
	TimestampNano int64     `json:"timestamp_nano"`
	RequestID     string    `json:"request_id"`
	Source        string    `json:"source"`

	*Billing        `json:"billing,omitempty"`
	*Monitoring     `json:"monitoring,omitempty"`
	*Audit          `json:"audit,omitempty"`
	*Search         `json:"search,omitempty"`
	*ForgotPassword `json:"forgot_password,omitempty"`
}
