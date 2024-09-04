package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type OrderPlacer struct {
	producer   *kafka.Producer
	topic      string
	deliveryCh chan kafka.Event
}

func NewOrderProducer(p *kafka.Producer, topic string) *OrderPlacer {
	return &OrderPlacer{
		producer:   p,
		topic:      topic,
		deliveryCh: make(chan kafka.Event, 10000),
	}
}

func (op *OrderPlacer) placeOrder(orderType string, size int) error {
	var (
		format  = fmt.Sprintf("%s - %d", orderType, size)
		payload = []byte(format)
	)

	err := op.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &op.topic,
			Partition: kafka.PartitionAny,
		},
		Value: payload,
	},
		op.deliveryCh, // to make sure that its delivered
	)

	if err != nil {
		log.Fatal(err)
	}

	<-op.deliveryCh
	fmt.Printf("Order placed: %s\n", format)
	return nil
}

func main() {
	topic := "HVSE"

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "foo",
		"acks":              "all"}) // ensures that all in-sync replicas must acknowledge the message before it's considered successfully sent.

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		return
	}

	fmt.Printf("%+v\n", p)

	op := NewOrderProducer(p, topic)
	for i := 0; i < 1000; i++ {
		if err := op.placeOrder("market", i+1); err != nil {
			log.Fatal(err)
		}
		time.Sleep(3 * time.Second)
	}

}

// go func() {
// 	for e := range p.Events() {
// 		switch ev := e.(type) {
// 		case *kafka.Message:
// 			if ev.TopicPartition.Error != nil {
// 				fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
// 			} else {
// 				fmt.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
// 					*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
// 			}
// 		}
// 	}
// }()
