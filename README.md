# gofiber_with_kafka

# Go Fiber with Kafka Example

This repository demonstrates how to integrate Go Fiber, a fast and lightweight web framework, with Kafka, a distributed streaming platform, for building real-time applications.

## Prerequisites

* Go (1.18 or later)
* Docker (for running Kafka and Zookeeper)
* Docker Compose (optional, but recommended)

## Getting Started

1.  **Clone the repository:**

    ```bash
    git clone <repository_url>
    cd gofiber_with_kafka
    ```

2.  **Start Kafka and Zookeeper using Docker Compose (recommended):**

    ```bash
    docker-compose up -d
    ```

    Alternatively, you can start Kafka and Zookeeper manually if you have them installed.

3.  **Install Go dependencies:**

    ```bash
    go mod tidy
    ```

4.  **Configure Kafka:**

    Update the `producer/main.go` file with your Kafka broker address and topic name:

    ```go
    package main

    const (
        KafkaBrokerAddress = "localhost:9092" // or your Kafka broker address
        KafkaTopic         = "my_topic"      // your Kafka topic
    )
    ```

5.  **Run the application:**

    ```bash
    go run producer/main.go
    go run worker/main.go
    ```

    The Fiber server will start, typically on port 3000.

## Example Usage

* **Producer messages:**

    Send a POST request to `producer/main.go` with a JSON payload to publish a message to Kafka.

    ```bash
    curl -X POST -H "Content-Type: application/json" -d '{"message": "Hello, Kafka!"}' http://localhost:3000/publish
    ```
                                               OR
    
    Run Postman application
       Select Method POST Past This URL(http://localhost:3000/publish)
       Select body
            Select x-www-form-urlencoded
            In key = message
            In val = any text
    send


* **Worker messages:**

    This example focuses on producing, but a consumer example would be similar, using a kafka library to create a consumer and read the messages from the defined topic.

## Code Structure

* `producer/main.go`: The main application file that sets up the Fiber server and Kafka producer also Contains configuration settings for Kafka. Contains the Kafka producer logic.
* `worker/main.go`: Contains the HTTP handlers for publishing messages.

## Key Components

* **Go Fiber:** Used to create the HTTP API for publishing messages.
* **Sarama:** A Go library for interacting with Kafka.
* **Docker Compose:** Used to simplify the setup of Kafka and Zookeeper.

## Kafka Producer Implementation

The `worker/main.go` file demonstrates how to create a Kafka producer using the Sarama library.

```go
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


