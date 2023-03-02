// Package lock provides a client library for using a DynamoDB table as a distributed locking mechansim.
//
// The entrypoint is the Locker interface and the constructor NewLocker.
package lock

import (
	"context"
	"fmt"
	"time"

	"github.com/Clever/kayvee-go/v7/logger"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// Locker is an interface for acquiring and releasing locks on keys with support for automatic lock expiration via leases.
//
// Locker knows about three kinds of resources: keys, owners, and locks:
//
// - A key is just a string, as far as locker is concerned. A client can attach its own meaning to the key.
//
// - Likewise an owner is just a string. A client can attach its own meaning and format to owner strings, and should ensure different logical units (goroutines or whatnot) have different owner strings.
//
// - A lock is a representation of exclusive access for a particular owner to a particular key. It has its own type: Lock.
//
// All locks have an end time at which they are considered no longer valid. The period of time in which a lock is valid is called the "lease."
// Leases prevent a lock from being held indefinitely by an owner which has crashed or otherwise finished without releasing the lock.
// Leases are stored as a specific point in time. Clients are responsible for ensuring that their clocks are synchronized; a small grace period allows small (~seconds) desyncs.
//
// The authoritative way to determine if a lock is still valid is to talk to the server by attempting to heartbeat the lock.
type Locker interface {
	// AcquireLock attepts to get the lock for a given input key. A lock can be acquired if there is not an active lease by a different owner.
	// If there is an active lease by the same owner as provided in the input, Acquire will succeed and the active lease will be replaced by input's lease.
	// Key and Owner are required inputs, as well as exactly one of LeaseEnd or LeaseDuration
	// If LeaseDuration is provided, it will be converted into a LeaseEnd before sending the request to the backend.
	// Returns either a non-nil Lock and a nil error, or a nil lock and non-nil error.
	// The error will be of type UnavailableError when there is an active lease on the key by a different owner.
	// Other errors may indicate e.g. a network problem talking to the backend, a validation error in the input, etc.
	AcquireLock(ctx context.Context, input AcquireLockInput) (*Lock, error)

	// AcquireAnyLock is a thin wrapper around AcquireLock for attepting to lock any one of a list of keys.
	// Underneath, it simply attempts to lock them one at a time and returns the first successful lock or an error.
	// It returns UnavailableError only when every single key was unavailable.
	AcquireAnyLock(ctx context.Context, input AcquireAnyLockInput) (*Lock, error)

	// HeartbeatLock is similar to AcquireLock except that it also fails if another owner held the lock in between acquiring the original lock and now, even if they have since released it.
	// In particular, Heartbeat succeeds in refreshing the lock if the lock's lease has expired but no one else used the lock in the mean time.
	HeartbeatLock(ctx context.Context, input HeartbeatLockInput) (*Lock, error)

	// StartBackgroundHeartbeats starts a goroutine to heartbeat the lock at set intervals.
	// It can be used to extend a lease during a long-running execution.
	// It stops if its context is cancelled, or if it gets an error. It reports the error via returned channel.
	// The lock pointer passed to it will be updated each time the heartbeat succeeds.
	// Since the lock isn't behind a mutex, take care if updating it manually after calling this.
	StartBackgroundHeartbeats(ctx context.Context, lockPointer *Lock, heartbeatInterval time.Duration, leaseDuration time.Duration) <-chan error

	// A lock can be released so long as it isn't owned by someone else i.e. if lock.Owner could acquire the lock.
	// That means that is is NOT an error to release a lock that wasn't owned by anyone.
	// An error means that the lock was NOT released. Either it is currently held by someone else, or there was a system error trying to release the lock.
	// If the lock is held by someone else, that's generally not retryable, and that is represented by the error type UnavailableError. In this case, the caller can be certain that the caller does not still hold the lock.
	// Any other error type could indicate that caller still holds the lock e.g. if there were a dynamo system error.
	ReleaseLock(ctx context.Context, lock Lock) error

	// GetCurrentLock will return the current lock for a key.  This is meant to help troubleshoot lock starvation.
	// If there is no lock, nil will be returned for both lock and error
	// This should not be used to "pre-check" for a lock as AcquireLock may still fail.
	GetCurrentLock(ctx context.Context, key string) (*Lock, error)
}

// Lock is a representation of a row in the lock table.
type Lock struct {
	// Note: these struct tags need to match the values for attribute names in lock.go
	Key         string    `dynamodbav:"key"`
	Owner       string    `dynamodbav:"owner"`
	LeasedUntil time.Time `dynamodbav:"leasedUntil"`
	CreatedAt   time.Time `dynamodbav:"createdAt"`
	TTL         time.Time `dynamodbav:"ttl"`
}

// NewLocker returns a Locker which talks to the given DynamoDB table using the given DynamoDBAPI, and logs to the given logger.
func NewLocker(ddbAPI dynamodbiface.DynamoDBAPI, tableName string, logger logger.KayveeLogger) Locker {
	return &locker{
		dynamoDBAPI: ddbAPI,
		tableName:   tableName,
		logger:      logger,
	}
}

// UnavailableError represents an attempt to get or update a lock has a different owner
type UnavailableError struct {
	Err error
}

func (l UnavailableError) Error() string {
	return fmt.Sprintf("lock unavailable: %v", l.Err)
}

// Unwrap lets this work with errors.Is and errors.As
func (l UnavailableError) Unwrap() error {
	return l.Err
}

// AcquireLockInput is the input to AcquireLock
type AcquireLockInput struct {
	// Key is required
	Key string
	// Owner is required
	Owner string

	// Exactly one of LeaseEnd and LeaseDuration should be provided, the other nil
	// LeaseEnd is the time at which the lease should expire.
	LeaseEnd *time.Time
	// LeaseDuration is a convenience field that will be converted into a LeaseEnd by adding it to time.Now() before sending to the backend
	LeaseDuration *time.Duration
}

// AcquireAnyLockInput is the input to AcquireAnyLock
type AcquireAnyLockInput struct {
	// Keys is required
	Keys []string
	// Owner is Required
	Owner string

	// RandomizeOrder is optional (defaults to false). If true, Locker will attempt to lock each key in a random order.
	RandomizeOrder *bool

	// Exactly one of LeaseEnd and LeaseDuration should be provided, the other nil
	// LeaseEnd is the time at which the lease should expire.
	LeaseEnd *time.Time
	// LeaseDuration is a convenience field that will be converted into a LeaseEnd by adding it to time.Now() before sending to the backend
	LeaseDuration *time.Duration
}

// HeartbeatLockInput is the input to AcquireLock
type HeartbeatLockInput struct {
	// Note that the LeasedUntil field is ignored, but Owner and Key must be correct.
	OriginalLock Lock

	// Exactly one of LeaseEnd and LeaseDuration should be provided, the other nil
	// They work the same as in AcquireLockInput
	// LeaseEnd is the time at which the lease should expire.
	LeaseEnd *time.Time
	// LeaseDuration is a convenience field that will be converted into a LeaseEnd by adding it to time.Now() before sending to the backend
	LeaseDuration *time.Duration
}
