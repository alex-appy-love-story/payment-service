package app

import (
	"fmt"
	"os"
	"strconv"
)

type DatabaseConfig struct {
	User         string
	Password     string
	Address      string
	DatabaseName string
}

type Config struct {
	RedisAddress   string
	QueueConfig    QueueConfig
	DatabaseConfig DatabaseConfig
	WorkerCount    int
	OrderSvcAddr   string
	OtelConfig     OtelConfig
}

type OtelConfig struct {
	ExporterEndpoint string
	Insecure         string
}

type QueueConfig struct {
	Server   string
	Next     string
	Previous string
}

// Required Configs:
// - REDIS_ADDR
// - DB_ADDRESS
// - DB_USER
// - DB_PASSWORD
// - WORKER_COUNT
func LoadConfig() (*Config, error) {
	cfg := &Config{
		RedisAddress: "localhost:6379",
		DatabaseConfig: DatabaseConfig{
			User:     "user",
			Password: "password",
			Address:  "localhost:3306",
		},
		OrderSvcAddr: "localhost:5001",
		WorkerCount:  5,
		OtelConfig: OtelConfig{
			ExporterEndpoint: "localhost:4317",
			Insecure:         "true",
		},
	}

	if redisAddr, exists := os.LookupEnv("REDIS_ADDR"); exists {
		cfg.RedisAddress = redisAddr
	}

	if orderSvcAddr, exists := os.LookupEnv("ORDER_SVC_ADDR"); exists {
		cfg.OrderSvcAddr = orderSvcAddr
	}

	if dbAddress, exists := os.LookupEnv("DB_ADDRESS"); exists {
		cfg.DatabaseConfig.Address = dbAddress
	}

	if dbUser, exists := os.LookupEnv("DB_USER"); exists {
		cfg.DatabaseConfig.User = dbUser
	}

	if dbPassword, exists := os.LookupEnv("DB_PASSWORD"); exists {
		cfg.DatabaseConfig.Password = dbPassword
	}

	if dbName, exists := os.LookupEnv("DB_NAME"); exists {
		cfg.DatabaseConfig.DatabaseName = dbName
	}

	if otelExporter, exists := os.LookupEnv("OTEL_EXPORTER_OTLP_ENDPOINT"); exists {
		cfg.OtelConfig.ExporterEndpoint = otelExporter
	}

	if workerCount, exists := os.LookupEnv("WORKER_COUNT"); exists {
		if val, err := strconv.Atoi(workerCount); err == nil {
			cfg.WorkerCount = val
		}
	}

	if serverQueueName, exists := os.LookupEnv("SERVER_QUEUE_NAME"); exists {
		cfg.QueueConfig.Server = serverQueueName
	} else {
		return nil, fmt.Errorf("Missing env 'SERVER_QUEUE_NAME'.")
	}

	if nextQueueName, exists := os.LookupEnv("NEXT_QUEUE_NAME"); exists {
		cfg.QueueConfig.Next = nextQueueName
	}

	if previousQueueName, exists := os.LookupEnv("PREVIOUS_QUEUE_NAME"); exists {
		cfg.QueueConfig.Previous = previousQueueName
	}

	return cfg, nil
}
