package app

import (
	"context"
	"fmt"

	"github.com/alex-appy-love-story/worker-template/circuitbreaker"
	"github.com/alex-appy-love-story/worker-template/tasks"
	"github.com/hibiken/asynq"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type App struct {
	Config         Config
	AsynqClient    *asynq.Client
	AsynqInspector *asynq.Inspector
	DBClient       *gorm.DB
	CircuitBreaker *circuitbreaker.CB
}

func New(config Config) *App {

	asynqConnection := asynq.RedisClientOpt{
		Addr: config.RedisAddress,
	}

	// NOTE(Appy): The DB client will be connected in Start().
	app := &App{
		Config:         config,
		AsynqClient:    asynq.NewClient(asynqConnection),
		AsynqInspector: asynq.NewInspector(asynqConnection),
		CircuitBreaker: circuitbreaker.NewCircuitBreaker(),
	}

	return app
}

func (a *App) connectDB(ctx context.Context) error {

	// Not required.
	if len(a.Config.DatabaseConfig.DatabaseName) == 0 {
		return nil
	}

	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		a.Config.DatabaseConfig.User,
		a.Config.DatabaseConfig.Password,
		a.Config.DatabaseConfig.Address,
		a.Config.DatabaseConfig.DatabaseName,
	)

	gorm, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})

	if err != nil {
		return err
	}

	a.DBClient = gorm
	return nil
}

func (a *App) Start(ctx context.Context) error {

	server := asynq.NewServer(
		asynq.RedisClientOpt{Addr: a.Config.RedisAddress},
		asynq.Config{
			Concurrency: 5,
			Queues: map[string]int{
				a.Config.QueueConfig.Server: 10,
			},
			BaseContext: func() context.Context {
				baseContext := context.Background()
				baseContext = context.WithValue(baseContext, "asynq_client", a.AsynqClient)
				baseContext = context.WithValue(baseContext, "db_client", a.DBClient)
				baseContext = context.WithValue(baseContext, "next_queue", a.Config.QueueConfig.Next)
				baseContext = context.WithValue(baseContext, "server_queue", a.Config.QueueConfig.Server)
				baseContext = context.WithValue(baseContext, "asynq_inspector", a.AsynqInspector)
				baseContext = context.WithValue(baseContext, "previous_queue", a.Config.QueueConfig.Previous)
				baseContext = context.WithValue(baseContext, "circuit_breaker", a.CircuitBreaker)
				return baseContext
			},
		},
	)

	fmt.Println("Successfully connected to redis!")

	if err := a.connectDB(ctx); err != nil {
		return err
	}

	if len(a.Config.DatabaseConfig.DatabaseName) == 0 {
		fmt.Println("No connection to db!")
	} else {
		fmt.Println("Successfully connected to db!")
	}

	defer func() {
		if err := a.AsynqClient.Close(); err != nil {
			fmt.Println("Failed to close redis", err)
		}
	}()

	fmt.Println("Starting server...")

	ch := make(chan error, 1)

	mux := asynq.NewServeMux()

	mux.Use(tasks.LoggingMiddleware)
	mux.Use(tasks.CircuitBreakerMiddleware)

	tasks.RegisterTopic(mux)

	go func() {
		err := server.Run(mux)
		if err != nil {
			ch <- fmt.Errorf("Failed to start server: %w", err)
		}
		close(ch)
	}()

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		server.Shutdown()
		return nil
	}
}
