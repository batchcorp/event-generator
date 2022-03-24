package events

import (
	"github.com/batchcorp/schemas/build/go/events/fakes"
)

type Topic struct {
	TopicName           string
	TopicReplicaCount   int
	TopicPartitionCount int
}

func GenerateTopicTestEvents(count int, prefix string, replicas, partitions int) []*fakes.Event {
	return nil
	//events := make([]*fakes.Event, 0)
	//
	//for i := 0; i < count; i++ {
	//	events = append(events, &fakes.Event{
	//		Type:      fakes.EventType_EVENT_TYPE_TOPIC_TEST,
	//		RequestId: uuid.NewV4().String(),
	//		Source:    "event-generator",
	//		//Topic: &Topic{
	//		//	TopicName:           prefix + "-" + uuid.NewV4().String(),
	//		//	TopicReplicaCount:   replicas,
	//		//	TopicPartitionCount: partitions,
	//		//},
	//	})
	//}
	//
	//return events
}
