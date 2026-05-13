package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/etuhoha/peril/internal/gamelogic"
	"github.com/etuhoha/peril/internal/pubsub"
	"github.com/etuhoha/peril/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	connectionString := "amqp://guest:guest@localhost:5672/"
	mqConnection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("could not connect to MQ: %v", err)
	}
	defer mqConnection.Close()
	fmt.Println("Connected to MQ...")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not log in: %v", err)
	}

	queueName := fmt.Sprintf("%v.%v", routing.PauseKey, username)
	pubsub.DeclareAndBind(mqConnection, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.QueueTypeTransient)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
}
