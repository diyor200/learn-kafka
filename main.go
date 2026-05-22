package main

import (
	"log"
	"os"

	"learn-kafka/consumer"
	"learn-kafka/producer"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: ./app [produce|consume]")
	}

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = "learn-topic"
	}

	switch os.Args[1] {
	case "produce":
		p := producer.New(brokers, topic)
		p.Run()
	case "consume":
		c := consumer.New(brokers, topic, "learn-group")
		c.Run()
	default:
		log.Fatalf("unknown command: %s (use produce or consume)", os.Args[1])
	}
}
