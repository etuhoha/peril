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

	state := gamelogic.NewGameState(username)
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
			for _, u := range move.Units {
				fmt.Printf("Move %v(%v) -> %v\n", u.Rank, u.ID, move.ToLocation)
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
