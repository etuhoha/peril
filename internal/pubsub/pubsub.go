package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"log"
	"time"

	"github.com/etuhoha/peril/internal/routing"
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

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(val)
	if err != nil {
		return err
	}

	pub := amqp.Publishing{ContentType: "application/gob", Body: buf.Bytes()}
	return ch.PublishWithContext(context.Background(), exchange, key, false, false, pub)
}

func PublishGameLog(ch *amqp.Channel, username, attacker, message string) AckType {
	msg := routing.GameLog{Username: username, Message: message, CurrentTime: time.Now()}
	err := PublishGob(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+attacker, msg)
	if err != nil {
		log.Printf("logging error: %v", err)
		return NackRequeue
	}

	return Ack
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
	return SubscribeGeneric(conn, exchange, queueName, key, queueType, handler, func(data []byte) (T, error) {
		var t T
		err := json.Unmarshal(data, &t)
		return t, err
	})
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	return SubscribeGeneric(conn, exchange, queueName, key, queueType, handler, func(data []byte) (T, error) {
		buf := bytes.NewBuffer(data)
		decoder := gob.NewDecoder(buf)
		var t T
		err := decoder.Decode(&t)
		return t, err
	})
}

func SubscribeGeneric[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
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
			t, err := unmarshaller(delivery.Body)
			if err != nil {
				log.Printf("error handling delivery from '%v': %v", queueName, err)
				continue // panic?
			}

			ack := handler(t)
			switch ack {
			case Ack:
				delivery.Ack(false)
			case NackRequeue:
				delivery.Nack(false, true)
			case NackDiscard:
				delivery.Nack(false, false)
			default:
				log.Fatalf("uncknown ack type %v\n", ack)
			}
		}
	}()

	return nil
}
