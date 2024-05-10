package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	broker = "pkc-p11xm.us-east-1.aws.confluent.cloud:9092"
	topic  = "example_1"
)

const (
	CLUSTER_API_KEY    = "***"
	CLUSTER_API_SECRET = "***"
)

func startProducer() error {
	//Producer configuration
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "PLAIN",
		"sasl.username":     CLUSTER_API_KEY,
		"sasl.password":     CLUSTER_API_SECRET,
		"acks":              "all",
	})

	if err != nil {
		fmt.Printf("Error creating the producer: %s\n", err)
		return err
	}
	defer producer.Close()

	// Produce messages
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)
	run := true

	fmt.Printf("Producer ready. (Ctrl+C to end the process):\n")

	count := 0

	for run {
		time.Sleep(1 * time.Second)
		count++

		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating...\n", sig)
			run = false
		default:
			text := fmt.Sprintf("This is a simple text tes message. %d", count)
			producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          []byte(text),
			}, nil)
			//producer.Flush(1000)
		}
	}
	count = 0

	return nil
}

func startConsumer() error {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          "mi-grupo",
		"auto.offset.reset": "earliest",

		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "PLAIN",
		"sasl.username":     CLUSTER_API_KEY,
		"sasl.password":     CLUSTER_API_SECRET,
	})
	if err != nil {
		fmt.Printf("Error creating Consumer: %s\n", err)
		return err
	}
	defer consumer.Close()

	// Suscribe to topic
	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		fmt.Printf("Error subscribing to topic: %s\n", err)
		return err
	}

	// Set up a channel for handling Ctrl-C, etc
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Process messages
	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev, err := consumer.ReadMessage(100 * time.Millisecond)

			if err != nil {
				// Errors are informational and automatically handled by the consumer
				continue
			}
			fmt.Printf("Consumed event from topic %s: key = %-10s value = %s\n",
				*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
		}
	}

	return nil
}

func main() {
	cnh := make(chan int)
	go startProducer()
	go startConsumer()
	<-cnh
}
