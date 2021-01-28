package batcher

import "github.com/google/uuid"

func getBatchID() string {
	return uuid.New().String()
}
