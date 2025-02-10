package main

import (
	"context"
	"log"

	"github.com/IBM/sarama"
)

const (
	kafkaBroker = "localhost:9092"
	topic       = "messages"
	groupID     = "worker-group"
)

func main() {

	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	consumerGroup, err := sarama.NewConsumerGroup([]string{kafkaBroker}, groupID, config)
	if err != nil {
		log.Fatal("Error creating Kafka consumer group:", err)
	}
	defer consumerGroup.Close()

	log.Println("Worker is listening for Kafka messages...")

	handler := &messageHandler{}
	for {
		err := consumerGroup.Consume(context.Background(), []string{topic}, handler)
		if err != nil {
			log.Println("Error consuming messages:", err)
		}
	}
}

type messageHandler struct{}

func (h *messageHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *messageHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *messageHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		log.Printf("Received message: %s", string(message.Value))
		session.MarkMessage(message, "")
	}
	return nil
}
