package lock

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/hashicorp/go-multierror"
	"gopkg.in/Clever/kayvee-go.v6/logger"
)

const gracePeriod = 5 * time.Second
const ttlDurationAfterLease = 2 * 24 * time.Hour

const (
	// the values of these need to match struct tag for Lock
	leaseEndAttribute = "leasedUntil"
	partitionKeyName  = "key"
)

type locker struct {
	dynamoDBAPI dynamodbiface.DynamoDBAPI
	tableName   string
	logger      logger.KayveeLogger
}

func (l locker) canAcquireLockExpression(owner string, leaseExpiredBefore time.Time) expression.Builder {
	// either:
	// - no item with PK exists
	// - item exists but lease < specified leaseExpiredBefore time
	// - item exists, and we own it

	doesntExist := expression.Name(partitionKeyName).AttributeNotExists()
	expired := expression.Name(leaseEndAttribute).LessThan(expression.Value(leaseExpiredBefore))
	ownedByUs := expression.Name("owner").Equal(expression.Value(owner))

	return expression.NewBuilder().WithCondition(expression.Or(doesntExist, expired, ownedByUs))
}

func (l locker) canHeartbeatLockExpression(owner string, leaseExpiredBefore time.Time) expression.Builder {
	// Lock must exist and be owned by us
	// Lease does not matter

	ownedByUs := expression.Name("owner").Equal(expression.Value(owner))

	return expression.NewBuilder().WithCondition(ownedByUs)
}

func (l locker) AcquireLock(ctx context.Context, input AcquireLockInput) (*Lock, error) {
	if input.LeaseDuration != nil && input.LeaseEnd != nil {
		return nil, fmt.Errorf("one of LeaseDuration or leaseEnd is required")
	}

	var leaseEnd time.Time
	if input.LeaseEnd != nil {
		leaseEnd = *input.LeaseEnd
	} else if input.LeaseDuration != nil {
		leaseEnd = time.Now().Add(*input.LeaseDuration)
	}

	newLock := Lock{
		Owner:       input.Owner,
		Key:         input.Key,
		LeasedUntil: leaseEnd,
		TTL:         leaseEnd.Add(ttlDurationAfterLease),
		CreatedAt:   time.Now(),
	}

	err := l.putLockIfAble(ctx, newLock)
	if err != nil {
		return nil, err
	}

	return &newLock, nil
}

func (l locker) AcquireAnyLock(ctx context.Context, input AcquireAnyLockInput) (*Lock, error) {
	var errs error

	keys := input.Keys
	if input.RandomizeOrder != nil && *input.RandomizeOrder {
		keys = randomizedCopy(input.Keys)
	}

	allLocksUnavailable := true
	for _, key := range keys {
		lock, err := l.AcquireLock(ctx, AcquireLockInput{
			Owner:         input.Owner,
			Key:           key,
			LeaseDuration: input.LeaseDuration,
			LeaseEnd:      input.LeaseEnd,
		})
		if err == nil {
			return lock, nil
		}
		if !errors.As(err, &UnavailableError{}) {
			allLocksUnavailable = false
		}
		errs = multierror.Append(errs, fmt.Errorf("locking key %s: %w", key, err))
	}
	if allLocksUnavailable {
		errs = UnavailableError{Err: errs}
	}
	return nil, errs
}

func (l locker) HeartbeatLock(ctx context.Context, input HeartbeatLockInput) (*Lock, error) {
	if input.LeaseDuration != nil && input.LeaseEnd != nil {
		return nil, fmt.Errorf("both LeaseDuration and leaseEnd were non-nil")
	}

	var leaseEnd time.Time
	if input.LeaseEnd != nil {
		leaseEnd = *input.LeaseEnd
	} else if input.LeaseDuration != nil {
		leaseEnd = time.Now().Add(*input.LeaseDuration)
	}

	newLock := Lock{
		Owner:       input.OriginalLock.Owner,
		Key:         input.OriginalLock.Key,
		LeasedUntil: leaseEnd,
		TTL:         leaseEnd.Add(ttlDurationAfterLease),
		CreatedAt:   time.Now(),
	}

	item, err := dynamodbattribute.MarshalMap(newLock)
	if err != nil {
		return nil, fmt.Errorf("failed to DynamoDB marshal Record, %v", err)
	}

	// In order to work around clock desynchronization a little, we allow a grace period:
	//   we'll only take a lease if it's been expired for gracePeriod duration
	leaseExpiredBefore := time.Now().Add(-gracePeriod)

	canHeartbeatExpression, err := l.canHeartbeatLockExpression(input.OriginalLock.Owner, leaseExpiredBefore).Build()
	if err != nil {
		return nil, fmt.Errorf("building condition expression to acquire lock: %v", err)
	}
	putInput := dynamodb.PutItemInput{
		TableName:                 aws.String(l.tableName),
		ConditionExpression:       canHeartbeatExpression.Condition(),
		ExpressionAttributeNames:  canHeartbeatExpression.Names(),
		ExpressionAttributeValues: canHeartbeatExpression.Values(),
		Item:                      item,
	}

	l.logger.DebugD("dynamo-operation", logger.M{
		"op":    "PutItem",
		"input": putInput,
	})

	_, err = l.dynamoDBAPI.PutItemWithContext(ctx, &putInput)

	var ccfe *dynamodb.ConditionalCheckFailedException
	if errors.As(err, &ccfe) {
		return nil, UnavailableError{Err: ccfe}
	} else if err != nil {
		l.logger.ErrorD("unexpected-dynamo-error", logger.M{
			"error":   err,
			"message": err.Error(),
			"type":    fmt.Sprintf("%T", err),
			"op":      "PutItem",
			"input":   putInput,
		})
		return nil, err
	}
	return &newLock, nil
}

func (l locker) ReleaseLock(ctx context.Context, lock Lock) error {
	return l.deleteLockIfAble(ctx, Lock{
		Key:   lock.Key,
		Owner: lock.Owner,
	})
}

func (l locker) putLockIfAble(ctx context.Context, lock Lock) error {
	item, err := dynamodbattribute.MarshalMap(lock)
	if err != nil {
		return fmt.Errorf("failed to DynamoDB marshal Record, %v", err)
	}

	// In order to work around clock desynchronization a little, we allow a grace period:
	//   we'll only take a lease if it's been expired for gracePeriod duration
	leaseExpiredBefore := time.Now().Add(-gracePeriod)

	canAcquireExpression, err := l.canAcquireLockExpression(lock.Owner, leaseExpiredBefore).Build()
	if err != nil {
		return fmt.Errorf("building condition expression to acquire lock: %v", err)
	}
	putInput := dynamodb.PutItemInput{
		TableName:                 aws.String(l.tableName),
		ConditionExpression:       canAcquireExpression.Condition(),
		ExpressionAttributeNames:  canAcquireExpression.Names(),
		ExpressionAttributeValues: canAcquireExpression.Values(),
		Item:                      item,
	}

	l.logger.DebugD("dynamo-operation", logger.M{
		"op":    "PutItem",
		"input": putInput,
	})

	_, err = l.dynamoDBAPI.PutItemWithContext(ctx, &putInput)

	var ccfe *dynamodb.ConditionalCheckFailedException
	if errors.As(err, &ccfe) {
		return UnavailableError{Err: ccfe}
	} else if err != nil {
		l.logger.ErrorD("unexpected-dynamo-error", logger.M{
			"error":   err,
			"message": err.Error(),
			"type":    fmt.Sprintf("%T", err),
			"op":      "PutItem",
			"input":   putInput,
		})
		return err
	}

	return nil
}

func (l locker) deleteLockIfAble(ctx context.Context, lock Lock) error {

	// In order to work around clock desynchronization a little, we allow a grace period:
	//   we'll only take a lease if it's been expired for gracePeriod duration
	leaseExpiredBefore := time.Now().Add(-gracePeriod)

	canAcquireExpression, err := l.canAcquireLockExpression(lock.Owner, leaseExpiredBefore).Build()
	if err != nil {
		return fmt.Errorf("building condition expression to acquire lock: %v", err)
	}
	deleteInput := dynamodb.DeleteItemInput{
		TableName:                 aws.String(l.tableName),
		ConditionExpression:       canAcquireExpression.Condition(),
		ExpressionAttributeNames:  canAcquireExpression.Names(),
		ExpressionAttributeValues: canAcquireExpression.Values(),
		Key: map[string]*dynamodb.AttributeValue{
			partitionKeyName: &dynamodb.AttributeValue{
				S: &lock.Key,
			},
		},
	}

	l.logger.DebugD("dynamo-operation", logger.M{
		"op":    "DeleteItem",
		"input": deleteInput,
	})

	_, err = l.dynamoDBAPI.DeleteItemWithContext(ctx, &deleteInput)

	var ccfe *dynamodb.ConditionalCheckFailedException
	if errors.As(err, &ccfe) {
		return UnavailableError{Err: ccfe}
	} else if err != nil {
		l.logger.ErrorD("unexpected-dynamo-error", logger.M{
			"error":   err,
			"message": err.Error(),
			"type":    fmt.Sprintf("%T", err),
			"op":      "PutItem",
			"input":   deleteInput,
		})
		return err
	}

	return nil
}

func (l locker) StartBackgroundHeartbeats(ctx context.Context, lock *Lock, heartbeatInterval time.Duration, leaseDuration time.Duration) <-chan error {
	errChan := make(chan error)
	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				newLock, err := l.HeartbeatLock(ctx, HeartbeatLockInput{
					OriginalLock:  *lock,
					LeaseDuration: &leaseDuration,
				})
				if err != nil {
					errChan <- fmt.Errorf("heartbeat error: %w", err)
					return
				}
				*lock = *newLock
			case <-ctx.Done():
				close(errChan)
				return
			}
		}
	}()
	return errChan
}

func randomizedCopy(keys []string) []string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	randomizedKeys := make([]string, len(keys))
	copy(randomizedKeys, keys)

	r.Shuffle(len(keys), func(i, j int) {
		randomizedKeys[i], randomizedKeys[j] = randomizedKeys[j], randomizedKeys[i]
	})
	return randomizedKeys
}

func (l locker) GetCurrentLock(ctx context.Context, key string) (*Lock, error) {
	getInput := dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			partitionKeyName: &dynamodb.AttributeValue{
				S: &key,
			},
		},
		TableName:      aws.String(l.tableName),
		ConsistentRead: aws.Bool(true),
	}

	result, err := l.dynamoDBAPI.GetItemWithContext(ctx, &getInput)

	if err != nil {
		l.logger.ErrorD("unexpected-dynamo-error", logger.M{
			"error":   err,
			"message": err.Error(),
			"type":    fmt.Sprintf("%T", err),
			"op":      "GetCurrentLock",
			"input":   getInput,
		})
		return nil, err
	}

	if len(result.Item) == 0 {
		return nil, nil
	}

	var lock Lock
	if unmarshalErr := dynamodbattribute.UnmarshalMap(result.Item, &lock); unmarshalErr != nil {
		return nil, fmt.Errorf("failed to DynamoDB unmarshal Record, %v", unmarshalErr)
	}
	return &lock, nil
}
