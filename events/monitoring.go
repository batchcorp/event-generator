package events

//import (
//	"time"
//
//	"github.com/brianvoe/gofakeit/v6"
//	uuid "github.com/satori/go.uuid"
//)
//
//const (
//	MonitoringProtocolTCP MonitoringProtocol = "TCP"
//	MonitoringProtocolUDP MonitoringProtocol = "UDP"
//	MonitoringProtoHTTP   MonitoringProtocol = "HTTP"
//
//	MonitoringStatusOK      MonitoringStatus = "OK"
//	MonitoringStatusError   MonitoringStatus = "ERROR"
//	MonitoringStatusWarning MonitoringStatus = "WARNING"
//)
//
//type MonitoringType string
//type MonitoringStatus string
//type MonitoringProtocol string
//
//type MonitoringTarget struct {
//	Address             string             `json:"address"`
//	Port                int                `json:"port"`
//	Type                MonitoringType     `json:"type"`
//	Protocol            MonitoringProtocol `json:"protocol"`
//	TimeoutSeconds      int                `json:"timeout_seconds"`
//	WarningThresholdMS  int                `json:"warning_threshold_ms"`
//	CriticalThresholdMS int                `json:"critical_threshold_ms"`
//	Metadata            map[string]string  `json:"metadata"`
//}
//
//type MonitoringResult struct {
//	Status    MonitoringStatus  `json:"status"`
//	Message   string            `json:"message"`
//	LatencyMS float64           `json:"latency_ms"`
//	Metadata  map[string]string `json:"metadata"`
//}
//
//type Monitoring struct {
//	RequestID string `json:"request_id"`
//	Source    string `json:"source"`
//	Timestamp int    `json:"timestamp"`
//
//	Target *MonitoringTarget `json:"target"`
//	Result *MonitoringResult `json:"result"`
//}
//
//func GenerateMonitoringEvents(count int) []*Event {
//	events := make([]*Event, 0)
//
//	for i := 0; i < count; i++ {
//		event := &Monitoring{}
//		event.Fill()
//
//		events = append(events, &Event{
//			Type:          MonitoringEventType,
//			TimestampNano: time.Now().UTC().UnixNano(),
//			RequestID:     uuid.NewV4().String(),
//			Source:        gofakeit.AppName(),
//			Monitoring:    event,
//		})
//	}
//
//	return nil
//}
//
//func (m *Monitoring) Fill() {
//}
