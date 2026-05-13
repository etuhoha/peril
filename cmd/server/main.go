package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/etuhoha/peril/internal/pubsub"
	"github.com/etuhoha/peril/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	connectionString := "amqp://guest:guest@localhost:5672/"
	mqConnection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("could not connect to MQ: %v", err)
	}
	defer mqConnection.Close()
	fmt.Println("Connected to MQ...")

	mqChannel, err := mqConnection.Channel()
	if err != nil {
		log.Fatalf("could not create MQ channel: %v", err)
	}

	err = pubsub.PublishJSON(mqChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
	if err != nil {
		log.Fatalf("could not send to MQ channel: %v", err)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Printf("Stopping Peril server...")
}
