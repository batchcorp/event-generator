package events

import (
	uuid "github.com/satori/go.uuid"
)

type Topic struct {
	TopicName           string
	TopicReplicaCount   int
	TopicPartitionCount int
}

func GenerateTopicTestEvents(count int, prefix string, replicas, partitions int) []*Event {
	events := make([]*Event, 0)

	for i := 0; i < count; i++ {
		events = append(events, &Event{
			Type:      TopicTestType,
			RequestID: uuid.NewV4().String(),
			Source:    "event-generator",
			Topic: &Topic{
				TopicName:           prefix + "-" + uuid.NewV4().String(),
				TopicReplicaCount:   replicas,
				TopicPartitionCount: partitions,
			},
		})
	}

	return events
}
