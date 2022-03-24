package events

//import (
//	"time"
//
//	"github.com/brianvoe/gofakeit/v6"
//	uuid "github.com/satori/go.uuid"
//)
//
//const (
//	ActiveResetPasswordStatus  ResetPasswordStatus = "unused"
//	UsedResetPasswordStatus    ResetPasswordStatus = "used"
//	InvalidResetPasswordStatus ResetPasswordStatus = "invalid"
//)
//
//type ResetPasswordStatus string
//
//type ForgotPassword struct {
//	Status          ResetPasswordStatus `json:"status"`
//	UserID          string              `json:"user_id"`
//	RequestDate     time.Time           `json:"request_date"`
//	RequestSourceIP string              `json:"request_source_ip"`
//	ResetSourceIP   string              `json:"reset_source_ip"`
//	ResetDate       time.Time           `json:"reset_date"`
//	Email           string              `json:"email"`
//	ExpirationDays  int                 `json:"expiration_days"`
//}
//
//func GenerateForgotPasswordEvents(count int) []*Event {
//	events := make([]*Event, 0)
//
//	for i := 0; i < count; i++ {
//		event := &ForgotPassword{}
//		event.Fill()
//
//		events = append(events, &Event{
//			Type:           ForgotPasswordType,
//			TimestampNano:  time.Now().UTC().UnixNano(),
//			RequestID:      uuid.NewV4().String(),
//			Source:         gofakeit.AppName(),
//			ForgotPassword: event,
//		})
//	}
//
//	return nil
//}
//
//func (f *ForgotPassword) Fill() {
//}
