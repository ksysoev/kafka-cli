package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

func main() {
	if len(os.Args) == 1 {
		fmt.Println("Usage: kafka-cli command [args]")
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "consume":
		Consume(os.Args[2:])
	case "produce":
		Produce(os.Args[2:])
	default:
		fmt.Println("Usage: kafka-cli command [args]")
		fmt.Println("Available commands: consume, produce")
		os.Exit(1)
	}
}

func getCredentials() (string, string, string) {
	username := os.Getenv("KAFKA_USERNAME")
	password := os.Getenv("KAFKA_PASSWORD")
	address := os.Getenv("KAFKA_ADDRESS")

	if username == "" || password == "" || address == "" {
		fmt.Println("KAFKA_USERNAME, KAFKA_PASSWORD and KAFKA_ADDRESS environment variables must be set")
		os.Exit(1)
	}

	return username, password, address
}

func Produce(args []string) {
	username, password, address := getCredentials()

	if len(args) != 2 {
		fmt.Println("Usage: kafka-cli produce <topic> <message>")
		os.Exit(1)
	}

	topic := args[0]
	message := args[1]

	mechanism, _ := scram.Mechanism(scram.SHA256, username, password)
	w := kafka.Writer{
		Addr:  kafka.TCP(address),
		Topic: topic,
		Transport: &kafka.Transport{
			SASL: mechanism,
			TLS:  &tls.Config{},
		},
	}

	err := w.WriteMessages(context.Background(), kafka.Message{Value: []byte(message)})
	w.Close()

	if err != nil {
		fmt.Println("Error sending message: " + err.Error())
		os.Exit(1)
	}

	fmt.Println("Message sent")
}

func Consume(args []string) {
	username, password, address := getCredentials()

	if len(args) != 1 {
		fmt.Println("Usage: kafka-cli consume <topic>")
		os.Exit(1)
	}

	topic := args[0]

	mechanism, _ := scram.Mechanism(scram.SHA256, username, password)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{address},
		GroupID: uuid.New().String(),
		Topic:   topic,
		Dialer: &kafka.Dialer{
			SASLMechanism: mechanism,
			TLS:           &tls.Config{},
		},
	})

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			fmt.Println("Error reading message: " + err.Error())
			os.Exit(1)
		}

		fmt.Println("Message received: ", m.Key, string(m.Value))
	}
}
