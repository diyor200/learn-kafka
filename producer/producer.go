package producer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	brokers string
	topic   string
	writer  *kafka.Writer
}

func New(brokers, topic string) *Producer {
	return &Producer{
		brokers: brokers,
		topic:   topic,
	}
}

func (p *Producer) init() {
	p.writer = &kafka.Writer{
		Addr:     kafka.TCP(p.brokers),
		Topic:    p.topic,
		Balancer: &kafka.LeastBytes{},
		Async:    false,
	}
}

func (p *Producer) Run() {
	p.init()

	ctx := context.Background()

	log.Printf("Producer connecting to %s on topic %q\n", p.brokers, p.topic)

	for {
		log.Println("Waiting for Kafka to be ready...")
		err := p.send(ctx, "ping", []byte("hello from kafka-producer"))
		if err == nil {
			break
		}
		log.Printf("  retrying: %v\n", err)
		time.Sleep(3 * time.Second)
	}

	log.Println("Kafka is ready — starting to produce messages")

	messages := []struct {
		key  string
		body string
	}{
		{"order-1", `{"order_id": 1, "item": "book", "qty": 2}`},
		{"order-2", `{"order_id": 2, "item": "laptop", "qty": 1}`},
		{"order-3", `{"order_id": 3, "item": "mouse", "qty": 4}`},
		{"order-4", `{"order_id": 4, "item": "keyboard", "qty": 1}`},
		{"order-5", `{"order_id": 5, "item": "monitor", "qty": 2}`},
	}

	for i, m := range messages {
		val := []byte(m.body)
		if err := p.send(ctx, m.key, val); err != nil {
			log.Fatalf("failed to send message %d: %v", i, err)
		}
		fmt.Printf("[PRODUCED]  key=%s  value=%s\n", m.key, m.body)
		time.Sleep(1 * time.Second)
	}

	for i := 6; i <= 10; i++ {
		key := fmt.Sprintf("order-%d", i)
		value := fmt.Sprintf(`{"order_id": %d, "item": "item-%d", "qty": %d}`, i, i, i)
		msg := kafka.Message{
			Key:   []byte(key),
			Value: []byte(value),
		}
		if err := p.writer.WriteMessages(ctx, msg); err != nil {
			log.Fatalf("write error: %v", err)
		}
		fmt.Printf("[PRODUCED]  key=%s  value=%s\n", key, value)
		time.Sleep(1 * time.Second)
	}

	log.Println("All messages sent. Producer exiting.")
	p.writer.Close()
}

func (p *Producer) send(ctx context.Context, key string, val []byte) error {
	msg := kafka.Message{
		Key:   []byte(key),
		Value: val,
	}
	return p.writer.WriteMessages(ctx, msg)
}
