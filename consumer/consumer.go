package consumer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	brokers string
	topic   string
	groupID string
}

func New(brokers, topic, groupID string) *Consumer {
	return &Consumer{
		brokers: brokers,
		topic:   topic,
		groupID: groupID,
	}
}

func (c *Consumer) Run() {
	log.Printf("Consumer connecting to %s on topic %q\n", c.brokers, c.topic)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{c.brokers},
		Topic:     c.topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
		MaxWait:   5 * time.Second,
	})
	reader.SetOffset(kafka.LastOffset)
	defer reader.Close()

	log.Println("Consumer ready — waiting for messages...")

	count := 0
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		msg, err := reader.ReadMessage(ctx)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				if count == 0 {
					log.Println("No messages received within timeout. Consumer exiting.")
				} else {
					log.Printf("No more messages. Total consumed: %d. Consumer exiting.", count)
				}
				break
			}
			log.Printf("read error: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		count++
		fmt.Printf("\n[CONSUMED]  #%d\n", count)
		fmt.Printf("  topic:     %s\n", msg.Topic)
		fmt.Printf("  partition: %d\n", msg.Partition)
		fmt.Printf("  offset:    %d\n", msg.Offset)
		fmt.Printf("  key:       %s\n", string(msg.Key))
		fmt.Printf("  value:     %s\n", string(msg.Value))
		fmt.Printf("  time:      %s\n", msg.Time.Format(time.RFC3339))
	}
}
