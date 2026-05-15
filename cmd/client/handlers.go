package main

import (
	"fmt"
	"log"
	"time"

	"github.com/etuhoha/peril/internal/gamelogic"
	"github.com/etuhoha/peril/internal/pubsub"
	"github.com/etuhoha/peril/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gameState *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gameState.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gameState *gamelogic.GameState, mqChan *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		out := gameState.HandleMove(move)
		switch out {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			playerSnap := gameState.GetPlayerSnap()
			war := gamelogic.RecognitionOfWar{Attacker: move.Player, Defender: playerSnap}
			err := pubsub.PublishJSON(
				mqChan,
				routing.ExchangePerilTopic,
				routing.WarRecognitionsPrefix+"."+playerSnap.Username,
				war)
			if err != nil {
				log.Printf("error declaring war: %v", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}

		return pubsub.NackDiscard
	}
}

func handlerWar(gameState *gamelogic.GameState, mqChan *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(war gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")

		outcome, winner, loser := gameState.HandleWar(war)
		msg := routing.GameLog{Username: gameState.GetUsername(), CurrentTime: time.Now()}
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			msg.Message = fmt.Sprintf("%v won a war against %v", winner, loser)
			return pubsub.PublishGameLog(mqChan, war.Attacker.Username, msg)
		case gamelogic.WarOutcomeYouWon:
			msg.Message = fmt.Sprintf("%v won a war against %v", winner, loser)
			return pubsub.PublishGameLog(mqChan, war.Attacker.Username, msg)
		case gamelogic.WarOutcomeDraw:
			msg.Message = fmt.Sprintf("A war between %v and %v resulted in a draw", winner, loser)
			return pubsub.PublishGameLog(mqChan, war.Attacker.Username, msg)
		}

		log.Printf("unknown outcome %v", outcome)

		return pubsub.NackDiscard
	}
}
