package utils

import (
	"fmt"
	"testing"
	"time"
)

func TestBackoffService(t *testing.T) {
	bof := BackoffService(10 * time.Second)
	var waitTime time.Duration
	waitTime = bof(true)
	if waitTime != 0*time.Second {
		t.Errorf("Invalid wait time for successful request: %s", waitTime)
	}
	waitTime = bof(false)
	if waitTime != 2*time.Second {
		t.Errorf("Invalid wait time after failed request: %s", waitTime)
	}
	waitTime = bof(false)
	if waitTime != 4*time.Second {
		t.Errorf("Invalid wait time after failed request: %s", waitTime)
	}
	waitTime = bof(false)
	if waitTime != 8*time.Second {
		t.Errorf("Invalid wait time after failed request: %s", waitTime)
	}
	waitTime = bof(false)
	if waitTime != 10*time.Second {
		t.Errorf("Invalid wait time after failed request: %s", waitTime)
	}
	waitTime = bof(true)
	if waitTime != 0*time.Second {
		t.Errorf("Invalid wait time after successful request: %s", waitTime)
	}
}

func ExampleBackoffService() {
	backoff := BackoffService(10 * time.Second)
	fmt.Println(backoff(false))
	fmt.Println(backoff(false))
	fmt.Println(backoff(false))
	fmt.Println(backoff(false))
	fmt.Println(backoff(true))
	fmt.Println(backoff(true))
	fmt.Println(backoff(false))
	// Output: 2s
	// 4s
	// 8s
	// 10s
	// 0s
	// 0s
	// 2s
}
