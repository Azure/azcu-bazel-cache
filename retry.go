package bazelazblob

import (
	"context"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
)

var defaultRetryConfig = retryConfig{
	maxRetries: 3,
	baseDelay:  100 * time.Millisecond,
	maxDelay:   30 * time.Second,
}

// retryConfig holds configuration for retry logic
type retryConfig struct {
	maxRetries int
	baseDelay  time.Duration
	maxDelay   time.Duration
}

// retryWithBackoff executes a function with retry logic that respects Azure's Retry-After header
// Falls back to exponential backoff if no Retry-After header is present
func retryWithBackoff(ctx context.Context, config retryConfig, operation func() error) error {
	var lastErr error

	for attempt := 0; attempt <= config.maxRetries; attempt++ {
		if attempt > 0 {
			var delay time.Duration

			// Try to extract Retry-After from the last error
			if retryAfter := extractRetryAfter(lastErr); retryAfter > 0 {
				delay = min(retryAfter, config.maxDelay)
			} else {
				// Fallback to exponential backoff with jitter
				delay = min(time.Duration(float64(config.baseDelay)*math.Pow(2, float64(attempt-1))), config.maxDelay)

				// Add jitter (±25% of delay)
				jitter := time.Duration(rand.Float64() * float64(delay) * 0.5)
				delay = delay + jitter - time.Duration(float64(delay)*0.25)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				// Continue with retry
			}
		}

		err := operation()
		if err == nil {
			return nil
		}

		lastErr = err

		// Check if this is a retryable error (429 or 503)
		if !isRetryableError(err) {
			break
		}
	}

	return lastErr
}

// extractRetryAfter attempts to extract the Retry-After duration from Azure error responses
func extractRetryAfter(err error) time.Duration {
	if err == nil {
		return 0
	}

	// Primary method: Extract from ResponseError headers
	if responseErr, ok := err.(*azcore.ResponseError); ok {
		if retryAfter := responseErr.RawResponse.Header.Get("Retry-After"); retryAfter != "" {
			if seconds := parseRetryAfterSeconds(retryAfter); seconds > 0 {
				return time.Duration(seconds) * time.Second
			}
		}
	}

	// Fallback: Try to parse from error message string (less reliable)
	errStr := err.Error()
	if strings.Contains(errStr, "Retry-After:") {
		if idx := strings.Index(errStr, "Retry-After:"); idx != -1 {
			afterPart := errStr[idx+len("Retry-After:"):]
			if endIdx := strings.IndexAny(afterPart, "\n\r,"); endIdx != -1 {
				afterPart = afterPart[:endIdx]
			}

			afterPart = strings.TrimSpace(afterPart)
			if seconds := parseRetryAfterSeconds(afterPart); seconds > 0 {
				return time.Duration(seconds) * time.Second
			}
		}
	}

	return 0
}

// parseRetryAfterSeconds parses Retry-After header value (in seconds)
func parseRetryAfterSeconds(value string) int {
	value = strings.TrimSpace(value)

	// Parse as integer (seconds)
	if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
		return seconds
	}

	// Could also be HTTP-date format, but Azure typically uses seconds
	// For simplicity, we'll just handle the seconds format
	return 0
}

// isRetryableError determines if an error should trigger a retry using proper type assertions
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check for Azure ResponseError with specific status codes
	if responseErr, ok := err.(*azcore.ResponseError); ok {
		statusCode := responseErr.StatusCode
		return statusCode == 429 || // Too Many Requests
			statusCode == 503 || // Service Unavailable
			statusCode == 500 // Internal Server Error (sometimes retryable)
	}

	// Fallback to string matching for edge cases where type assertion fails
	errStr := err.Error()
	return strings.Contains(errStr, "429") ||
		strings.Contains(errStr, "Too Many Requests") ||
		strings.Contains(errStr, "503") ||
		strings.Contains(errStr, "Service Unavailable") ||
		strings.Contains(errStr, "ServerBusy")
}
