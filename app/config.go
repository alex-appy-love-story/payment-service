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

		WorkerCount: 5,
	}

	if redisAddr, exists := os.LookupEnv("REDIS_ADDR"); exists {
		cfg.RedisAddress = redisAddr
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
