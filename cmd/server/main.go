package main

import (
	"fmt"
	"log"
	"os"

	"github.com/etuhoha/peril/internal/gamelogic"
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
	fmt.Println("Connected to MQ.")

	mqChannel, err := mqConnection.Channel()
	if err != nil {
		log.Fatalf("could not create MQ channel: %v", err)
	}

	_, logQueue, err := pubsub.DeclareAndBind(
		mqConnection,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.QueueTypeDurable)
	if err != nil {
		log.Fatalf("could not create log queue: %v", err)
	}
	fmt.Printf("Log queue created: '%v'.\n", logQueue.Name)

	gamelogic.PrintServerHelp()

	for {
		input := gamelogic.GetInput()
		cmd := input[0]
		switch cmd {
		case "pause":
			fmt.Printf("Sending pause message...\n")
			err = pubsub.PublishJSON(mqChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				log.Fatalf("could not send 'pause': %v", err)
			}
		case "resume":
			fmt.Printf("Sending resume message...\n")
			err = pubsub.PublishJSON(mqChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				log.Fatalf("could not send 'resume': %v", err)
			}
		case "quit":
			fmt.Printf("Stopping Peril server...\n")
			os.Exit(0)
		default:
			fmt.Printf("unknown command '%v'\n", cmd)
		}
	}
}
