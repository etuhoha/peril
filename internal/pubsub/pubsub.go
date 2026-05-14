package pubsub

import (
	"context"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	bytes, err := json.Marshal(val)
	if err != nil {
		return err
	}

	pub := amqp.Publishing{ContentType: "application/json", Body: bytes}
	return ch.PublishWithContext(context.Background(), exchange, key, false, false, pub)
}

type SimpleQueueType int

const (
	QueueTypeDurable = iota
	QueueTypeTransient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	durable := queueType == QueueTypeDurable
	table := amqp.Table{"x-dead-letter-exchange": "peril_dlx"}
	queue, err := channel.QueueDeclare(queueName, durable, !durable, !durable, false, table)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = channel.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return channel, queue, nil
}

type AckType int

const (
	Ack = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	channel, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	deliveryChan, err := channel.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for delivery := range deliveryChan {
			var t T
			err := json.Unmarshal(delivery.Body, &t)
			if err != nil {
				log.Printf("error handling delivery from '%v': %v", queueName, err)
				continue // panic?
			}

			ack := handler(t)
			switch ack {
			case Ack:
				log.Println("ACK")
				delivery.Ack(false)
			case NackRequeue:
				log.Println("NACK requeue")
				delivery.Nack(false, true)
			case NackDiscard:
				log.Println("NACK discard")
				delivery.Nack(false, false)
			default:
				log.Fatalf("uncknown ack type %v\n", ack)
			}
		}
	}()

	return nil
}
