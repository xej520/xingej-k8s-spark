package retry

import (
	"fmt"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

var (
	// ErrRetryTimeout try timeout
	ErrRetryTimeout = errors.New("retry timeout error")
)

// RetryError type try error
type RetryError struct {
	retries int
}

func (r *RetryError) Error() string {
	return fmt.Sprintf("stail failling after %d retries", r.retries)
}

// ConditionFunc condition function
type ConditionFunc func() (bool, error)
type call func() error

// Retry try run
func Retry(maxRetries int, interval time.Duration, cf ConditionFunc) error {
	if maxRetries <= 0 {
		return fmt.Errorf("maxRetries (%d) should be > 0", maxRetries)
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()

	for i := 0; ; i++ {
		ok, err := cf()
		if ok {
			return nil
		}
		if i+1 == maxRetries {
			return err
		}
		<-tick.C
	}
	return &RetryError{maxRetries}
}

// RetryIfErr try if error
func RetryIfErr(timeout time.Duration, call call) error {
	c := make(chan interface{})
	go func() {
		defer close(c)
		for {
			select {
			case <-c:
				return
			default:
				if err := call(); err != nil {
					time.Sleep(time.Second)
					log.Warnf("Waiting for success: %v", err)
				} else {
					return
				}
			}
		}
	}()

	select {
	case <-c:
		return nil // completed normally
	case <-time.After(timeout):
		c <- 1
		return ErrRetryTimeout
	}
}

// IsRetryFailure try fail
func IsRetryFailure(err error) bool {
	_, ok := err.(*RetryError)
	return ok
}
