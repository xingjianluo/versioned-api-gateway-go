// Package errors 定义网关统一的可区分错误类型。
package errors

import "fmt"

// Kind 为错误类别，调用方可据此区分失败原因。
type Kind string

const (
	KindVersionRejected    Kind = "version_rejected"
	KindRateLimited        Kind = "rate_limited"
	KindUpstreamTimeout    Kind = "upstream_timeout"
	KindUpstreamConn       Kind = "upstream_connection_error"
	KindUpstreamOverload   Kind = "upstream_overloaded"
	KindCircuitOpen        Kind = "circuit_open"
	KindDegraded           Kind = "degraded"
	KindNonIdempotentRetry Kind = "non_idempotent_retry_denied"
	KindCallbackFailed     Kind = "callback_delivery_failed"
)

// GatewayError 携带错误类别、原因与是否可安全重试。
type GatewayError struct {
	Kind      Kind
	Reason    string
	RequestID string
	Retryable bool
	Attempts  int
}

func (e *GatewayError) Error() string {
	return fmt.Sprintf("[%s] request=%s attempts=%d retryable=%t: %s",
		e.Kind, e.RequestID, e.Attempts, e.Retryable, e.Reason)
}

// New 构造网关错误。
func New(kind Kind, reason, requestID string, retryable bool) *GatewayError {
	return &GatewayError{
		Kind:      kind,
		Reason:    reason,
		RequestID: requestID,
		Retryable: retryable,
	}
}
