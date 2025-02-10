package main

import (
	"log"
	"net/http"

	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
)

const (
	kafkaBroker = "localhost:9092"
	topic       = "messages"
)

func main() {

	app := fiber.New()

	app.Post("/publish", func(c *fiber.Ctx) error {
		msg := c.FormValue("message")
		if msg == "" {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Message cannot be empty"})
		}

		err := sendToKafka(msg)
		if err != nil {
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}

		return c.JSON(fiber.Map{"status": "Message sent to Kafka", "message": msg})
	})

	log.Println("Fiber server is running on http://localhost:3000")
	log.Fatal(app.Listen(":3000"))
}

func sendToKafka(message string) error {
	producer, err := sarama.NewSyncProducer([]string{kafkaBroker}, nil)
	if err != nil {
		return err
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	_, _, err = producer.SendMessage(msg)
	return err
}
