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
	fmt.Println("Starting Peril client...")

	connectionString := "amqp://guest:guest@localhost:5672/"
	mqConnection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("could not connect to MQ: %v\n", err)
	}
	defer mqConnection.Close()
	fmt.Println("Connected to MQ.")

	mqChannel, err := mqConnection.Channel()
	if err != nil {
		log.Fatalf("could not create MQ channel: %v", err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not log in: %v\n", err)
	}

	state := gamelogic.NewGameState(username)

	pauseQueueName := routing.PauseKey + "." + username
	err = pubsub.SubscribeJSON(
		mqConnection,
		routing.ExchangePerilDirect,
		pauseQueueName,
		routing.PauseKey,
		pubsub.QueueTypeTransient,
		handlerPause(state))

	if err != nil {
		log.Fatalf("could not subscribe to %v: %v\n", pauseQueueName, err)
	}

	moveQueueName := routing.ArmyMovesPrefix + "." + username
	err = pubsub.SubscribeJSON(
		mqConnection,
		routing.ExchangePerilTopic,
		moveQueueName,
		routing.ArmyMovesPrefix+".*",
		pubsub.QueueTypeTransient,
		handlerMove(state, mqChannel))
	if err != nil {
		log.Fatalf("could not subscribe to %v: %v\n", moveQueueName, err)
	}

	warQueueName := routing.WarRecognitionsPrefix
	err = pubsub.SubscribeJSON(
		mqConnection,
		routing.ExchangePerilTopic,
		warQueueName,
		routing.WarRecognitionsPrefix+".*",
		pubsub.QueueTypeDurable,
		handlerWar(state, mqChannel))
	if err != nil {
		log.Fatalf("could not subscribe to %v: %v\n", warQueueName, err)
	}

	for {
		cmd := gamelogic.GetInput()
		cmdName := cmd[0]
		switch cmdName {
		case "spawn":
			err = state.CommandSpawn(cmd)
			if err != nil {
				fmt.Printf("error executing '%v': %v\n", cmd, err)
			}
		case "move":
			move, err := state.CommandMove(cmd)
			if err != nil {
				fmt.Printf("error executing '%v': %v\n", cmd, err)
			}

			err = pubsub.PublishJSON(mqChannel, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+username, move)
			if err != nil {
				log.Fatalf("could not send the move: %v", err)
			}
		case "status":
			state.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet")
		case "quit":
			gamelogic.PrintQuit()
			os.Exit(0)
		default:
			fmt.Printf("unknown command '%v'\n", cmdName)
		}
	}
}
