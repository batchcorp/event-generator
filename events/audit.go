package events

//import (
//	"time"
//
//	"github.com/brianvoe/gofakeit/v6"
//	uuid "github.com/satori/go.uuid"
//)
//
//const (
//	GetAction    ActionType = "get"
//	CreateAction ActionType = "create"
//	DeleteAction ActionType = "delete"
//	UpdateAction ActionType = "update"
//)
//
//type ActionType string
//
//type Audit struct {
//	Action      ActionType        `json:"action"`
//	Target      string            `json:"target"`
//	UserID      string            `json:"user_id"`
//	Application string            `json:"application"`
//	Metadata    map[string]string `json:"metadata"`
//	Timestamp   int               `json:"timestamp"`
//}
//
//func GenerateAuditEvents(count int) []*Event {
//	events := make([]*Event, 0)
//
//	for i := 0; i < count; i++ {
//		event := &Audit{}
//		event.Fill()
//
//		events = append(events, &Event{
//			Type:          AuditEventType,
//			TimestampNano: time.Now().UTC().UnixNano(),
//			RequestID:     uuid.NewV4().String(),
//			Source:        gofakeit.AppName(),
//			Audit:         event,
//		})
//	}
//
//	return nil
//}
//
//func (a *Audit) Fill() {
//}
