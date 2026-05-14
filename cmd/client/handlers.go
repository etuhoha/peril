package main

import (
	"fmt"

	"github.com/etuhoha/peril/internal/gamelogic"
	"github.com/etuhoha/peril/internal/routing"
)

func handlerPause(gameState *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		gameState.HandlePause(ps)
	}
}
