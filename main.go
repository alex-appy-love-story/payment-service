package main

import (
	"context"
	"log"

	"github.com/alex-appy-love-story/worker-template/app"
	"github.com/joho/godotenv"
)

func printNumber(n int) int {
	return n
}

func main() {
	godotenv.Load()

	config, err := app.LoadConfig()

	if err != nil {
		log.Fatalln("Error:", err)
	}

	app := app.New(*config)
	app.Start(context.Background())
}
