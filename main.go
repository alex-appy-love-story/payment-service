package main

import (
	"context"
	"log"

	"github.com/alex-appy-love-story/worker-template/app"
)

func printNumber(n int) int {
	return n
}

func main() {
	config, err := app.LoadConfig()

	if err != nil {
		log.Fatalln("Error:", err)
	}

	app := app.New(*config)
	app.Start(context.Background())
}
