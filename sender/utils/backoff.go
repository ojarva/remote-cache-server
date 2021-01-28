package utils

import (
	"fmt"
	"math"
	"time"
)

// BackoffService provides a simple exponential backoff.
func BackoffService(maxWaitTime time.Duration) func(success bool) time.Duration {
	var failureCount int
	return func(success bool) time.Duration {
		if success {
			// We succeeded on the last iteration; it's ok to retry immediately.
			failureCount = 0
			return 0 * time.Second
		}
		failureCount++
		waitTime, _ := time.ParseDuration(fmt.Sprintf("%.0fs", math.Pow(2, float64(failureCount))))
		if waitTime > maxWaitTime {
			return maxWaitTime
		}
		return waitTime
	}
}
