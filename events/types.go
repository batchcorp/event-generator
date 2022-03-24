package events

const (
	BillingEventType    EventType = "billing"
	MonitoringEventType EventType = "monitoring"
	AuditEventType      EventType = "audit"
	SearchEventType     EventType = "search"
	ForgotPasswordType  EventType = "forgot_password"
	TopicTestType       EventType = "topic_test"
)

type EventType string
