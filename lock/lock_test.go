package lock

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gopkg.in/Clever/kayvee-go.v6/logger"
)

func TestLocker(t *testing.T) {
	if _, set := os.LookupEnv("INTEGRATION_TEST"); !set {
		t.Skip()
	}

	dynamoService := dynamodb.New(session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:   aws.String("us-west-1"),
			Endpoint: aws.String("http://localhost:8000"),
		},
	})))
	tableName := "EnvironmentLocks"

	// if the log is too noisy, call kvLogger.SetLogLevel(logger.Warning)
	kvLogger := logger.New("lock-client")
	locker := NewLocker(dynamoService, tableName, kvLogger)

	type testcase struct {
		description  string
		prepareTable func(*dynamodb.DynamoDB) error
		testFn       func(*testing.T)
	}

	testcases := []testcase{
		{
			description: "acquire and release on empty table",
			testFn: func(t *testing.T) {
				ctx := context.Background()
				duration := 5 * time.Minute
				input := AcquireLockInput{
					Key:           "key1",
					Owner:         "owner1",
					LeaseDuration: &duration,
				}
				l := AcquireAndValidate(ctx, t, locker, input)
				err := locker.ReleaseLock(ctx, *l)
				if err != nil {
					t.Fatalf("ReleaseLock error: %v", err)
				}

			},
		},
		{
			description: "acquire and release on an expired lease, different owner",
			testFn: func(t *testing.T) {
				ctx := context.Background()
				// create an expired lease
				duration := -5 * time.Minute
				input := AcquireLockInput{
					Key:           "key1",
					Owner:         "owner1",
					LeaseDuration: &duration,
				}
				AcquireAndValidate(ctx, t, locker, input)

				// try to reacquire, different owner
				duration = 5 * time.Minute
				input = AcquireLockInput{
					Key:           "key1",
					Owner:         "owner2",
					LeaseDuration: &duration,
				}
				AcquireAndValidate(ctx, t, locker, input)

			},
		},
		{
			description: "acquire and release on a released item",
			testFn: func(t *testing.T) {
				ctx := context.Background()
				duration := 5 * time.Minute
				input := AcquireLockInput{
					Key:           "key1",
					Owner:         "owner1",
					LeaseDuration: &duration,
				}
				l := AcquireAndValidate(ctx, t, locker, input)
				err := locker.ReleaseLock(ctx, *l)
				if err != nil {
					t.Fatalf("ReleaseLock error: %v", err)
				}

				// try to reacquire, different owner
				duration = 5 * time.Minute
				input = AcquireLockInput{
					Key:           "key1",
					Owner:         "owner2",
					LeaseDuration: &duration,
				}
				AcquireAndValidate(ctx, t, locker, input)

			},
		},
		{
			description: "acquire and then same owner reacquire",
			testFn: func(t *testing.T) {
				ctx := context.Background()
				duration := 5 * time.Minute
				input := AcquireLockInput{
					Key:           "key1",
					Owner:         "owner1",
					LeaseDuration: &duration,
				}

				duration = 2 * time.Minute
				input = AcquireLockInput{
					Key:           "key1",
					Owner:         "owner1",
					LeaseDuration: &duration,
				}
				AcquireAndValidate(ctx, t, locker, input)
			},
		},
		{
			description: "acquire and then different owner acquire",
			testFn: func(t *testing.T) {
				ctx := context.Background()
				duration := 5 * time.Minute
				input := AcquireLockInput{
					Key:           "key1",
					Owner:         "owner1",
					LeaseDuration: &duration,
				}

				AcquireAndValidate(ctx, t, locker, input)

				duration = 2 * time.Minute
				input = AcquireLockInput{
					Key:           "key1",
					Owner:         "owner2",
					LeaseDuration: &duration,
				}
				_, err := locker.AcquireLock(ctx, input)
				if err == nil {
					t.Fatalf("new owner was able to acquire unexpired lock")
				} else if _, ok := err.(UnavailableError); !ok {
					t.Fatalf("wrong error type when attempting to lock in-use lock")
				}
			},
		},
		{
			description: "releasing a lock we don't own",
			testFn: func(t *testing.T) {
				ctx := context.Background()
				duration := 5 * time.Minute
				input := AcquireLockInput{
					Key:           "key1",
					Owner:         "owner1",
					LeaseDuration: &duration,
				}

				AcquireAndValidate(ctx, t, locker, input)

				lock := Lock{
					Key:   "key1",
					Owner: "owner2",
				}

				err := locker.ReleaseLock(ctx, lock)
				if err == nil {
					t.Fatalf("new owner was able to release lock owned by someone else")
				} else if _, ok := err.(UnavailableError); !ok {
					t.Fatalf("wrong error type when attempting to release in-use lock")
				}
			},
		},
		{
			description: "heartbeating a lock we own",
			testFn: func(t *testing.T) {
				ctx := context.Background()
				duration := 5 * time.Minute
				input := AcquireLockInput{
					Key:           "key1",
					Owner:         "owner1",
					LeaseDuration: &duration,
				}

				AcquireAndValidate(ctx, t, locker, input)

				lock := Lock{
					Key:   "key1",
					Owner: "owner1",
				}

				newDuration := 10 * time.Minute
				newLock, err := locker.HeartbeatLock(ctx, HeartbeatLockInput{
					OriginalLock:  lock,
					LeaseDuration: &newDuration,
				})
				if err != nil {
					t.Fatalf("unable to heartbeat valid lock")
				}
				if err = ValidateLeaseEndFromDuration(newDuration, *newLock); err != nil {
					t.Fatalf("%v", err)
				}
				if err = ValidateTTL(*newLock); err != nil {
					t.Fatalf("%v", err)
				}
			},
		},
		{
			description: "heartbeating a lock we used to own",
			testFn: func(t *testing.T) {
				ctx := context.Background()
				duration := -5 * time.Minute
				input := AcquireLockInput{
					Key:           "key1",
					Owner:         "owner1",
					LeaseDuration: &duration,
				}

				AcquireAndValidate(ctx, t, locker, input)

				lock := Lock{
					Key:   "key1",
					Owner: "owner1",
				}

				newDuration := 10 * time.Minute
				newLock, err := locker.HeartbeatLock(ctx, HeartbeatLockInput{
					OriginalLock:  lock,
					LeaseDuration: &newDuration,
				})
				if err != nil {
					t.Fatalf("unable to heartbeat valid lock")
				}
				if err = ValidateLeaseEndFromDuration(newDuration, *newLock); err != nil {
					t.Fatalf("%v", err)
				}
				if err = ValidateTTL(*newLock); err != nil {
					t.Fatalf("%v", err)
				}
			},
		},
		{
			description: "heartbeating a lock that's been stolen",
			testFn: func(t *testing.T) {
				ctx := context.Background()
				duration := -5 * time.Minute
				input := AcquireLockInput{
					Key:           "key1",
					Owner:         "owner1",
					LeaseDuration: &duration,
				}

				lock := AcquireAndValidate(ctx, t, locker, input)

				duration *= -1
				input = AcquireLockInput{
					Key:           "key1",
					Owner:         "owner2",
					LeaseDuration: &duration,
				}
				AcquireAndValidate(ctx, t, locker, input)

				newDuration := 10 * time.Minute
				_, err := locker.HeartbeatLock(ctx, HeartbeatLockInput{
					OriginalLock:  *lock,
					LeaseDuration: &newDuration,
				})
				if err == nil {
					t.Fatalf("heartbeat succeeded on an invalid lock")
				} else if _, ok := err.(UnavailableError); !ok {
					t.Fatalf("wrong error type when attempting to release in-use lock")
				}
			},
		},
		{
			description: "heartbeating a lock that's been stolen but released",
			testFn: func(t *testing.T) {
				ctx := context.Background()
				duration := -5 * time.Minute
				input := AcquireLockInput{
					Key:           "key1",
					Owner:         "owner1",
					LeaseDuration: &duration,
				}

				lock := AcquireAndValidate(ctx, t, locker, input)

				duration *= -1
				input = AcquireLockInput{
					Key:           "key1",
					Owner:         "owner2",
					LeaseDuration: &duration,
				}
				stolenLock := AcquireAndValidate(ctx, t, locker, input)
				err := locker.ReleaseLock(ctx, *stolenLock)
				if err != nil {
					t.Fatalf("owner wasn't able to release lock")
				}

				newDuration := 10 * time.Minute
				_, err = locker.HeartbeatLock(ctx, HeartbeatLockInput{
					OriginalLock:  *lock,
					LeaseDuration: &newDuration,
				})
				if err == nil {
					t.Fatalf("heartbeat succeeded on an invalid lock")
				} else if _, ok := err.(UnavailableError); !ok {
					t.Fatalf("wrong error type when attempting to heartbeat in-use lock")
				}
			},
		},
		{
			description: "acquiring a lock sets CreatedAt",
			testFn: func(t *testing.T) {
				ctx := context.Background()
				duration := 5 * time.Minute
				input := AcquireLockInput{
					Key:           "key1",
					Owner:         "owner1",
					LeaseDuration: &duration,
				}
				now := time.Now()
				l := AcquireAndValidate(ctx, t, locker, input)
				assert.WithinDuration(t, now, l.CreatedAt, 500*time.Millisecond)
			},
		},
		{
			description: "heartbeating a lock we own resets created at",
			testFn: func(t *testing.T) {
				ctx := context.Background()
				duration := 5 * time.Minute
				input := AcquireLockInput{
					Key:           "key1",
					Owner:         "owner1",
					LeaseDuration: &duration,
				}

				AcquireAndValidate(ctx, t, locker, input)

				lock := Lock{
					Key:   "key1",
					Owner: "owner1",
				}
				time.Sleep(1 * time.Second)
				now := time.Now()
				newLock, _ := locker.HeartbeatLock(ctx, HeartbeatLockInput{
					OriginalLock:  lock,
					LeaseDuration: &duration,
				})

				assert.WithinDuration(t, now, newLock.CreatedAt, 500*time.Millisecond)
			},
		},
		{
			description: "getCurrentLock for existing lock",
			testFn: func(t *testing.T) {
				ctx := context.Background()
				duration := 5 * time.Minute
				input := AcquireLockInput{
					Key:           "key1",
					Owner:         "owner1",
					LeaseDuration: &duration,
				}

				lockedAt := time.Now()
				AcquireAndValidate(ctx, t, locker, input)

				lock, err := locker.GetCurrentLock(ctx, "key1")
				assert.Nil(t, err)
				assert.NotNil(t, lock)
				assert.Equal(t, "owner1", lock.Owner)
				assert.Equal(t, "key1", lock.Key)
				if err := ValidateLeaseEndFromDuration(*input.LeaseDuration, *lock); err != nil {
					t.Fatalf("%v", err)
				}
				if err = ValidateTTL(*lock); err != nil {
					t.Fatalf("%v", err)
				}
				assert.WithinDuration(t, lockedAt, lock.CreatedAt, 500*time.Millisecond)
			},
		},
		{
			description: "getCurrentLock for missing lock",
			testFn: func(t *testing.T) {
				ctx := context.Background()
				lock, err := locker.GetCurrentLock(ctx, "not-locked")
				assert.Nil(t, lock)
				assert.Nil(t, err)
			},
		},
	}

	for _, testcase := range testcases {
		err := createTable(t, dynamoService, tableName)
		if err != nil {
			t.Fatalf("creating table: %v", err)
		}
		if testcase.prepareTable != nil {
			testcase.prepareTable(dynamoService)
		}
		success := t.Run(testcase.description, testcase.testFn)
		if !success {
			s, _ := scanTable(dynamoService, tableName)
			t.Logf("table:\n%s", s)
		}
		err = deleteTable(t, dynamoService, tableName)
		if err != nil {
			t.Fatalf("delete table: %v", err)
		}
	}
}

func ValidateLeaseEndFromDuration(d time.Duration, lock Lock) error {
	leaseEndTime := time.Now().Add(d)
	if leaseEndTime.Add(-10*time.Second).After(lock.LeasedUntil) || leaseEndTime.Before(lock.LeasedUntil) {
		return fmt.Errorf("estimated lease end from input was %v; returned lock was %v",
			leaseEndTime, lock.LeasedUntil,
		)
	}
	return nil
}

func ValidateTTL(lock Lock) error {
	if lock.LeasedUntil.Add(ttlDurationAfterLease) != lock.TTL {
		return fmt.Errorf("lease end plus ttl duration didn't add up to ttl attribute: leasedUntil %v + ttl duration %v != TTL %v", lock.LeasedUntil, ttlDurationAfterLease, lock.TTL)
	}
	return nil
}

func AcquireAndValidate(ctx context.Context, t *testing.T, l Locker, input AcquireLockInput) *Lock {
	lock, err := l.AcquireLock(ctx, input)
	if err != nil {
		t.Fatalf("AcquireLock error: %v", err)
	}

	require.NotNil(t, lock, "AcquireLock should return a non-nil lock")
	require.Equal(t, lock.Owner, input.Owner, "lock should be owned by the callee")
	require.Equal(t, lock.Key, input.Key, "lock should have same key as requested by the callee")
	if input.LeaseEnd != nil && lock.LeasedUntil != *input.LeaseEnd {
		t.Fatalf("lock input has absolute lease end %v and got back lock with %v ",
			input.LeaseEnd, lock.LeasedUntil,
		)
	} else if input.LeaseDuration != nil {
		if err := ValidateLeaseEndFromDuration(*input.LeaseDuration, *lock); err != nil {
			t.Fatalf("%v", err)
		}
	}
	if err = ValidateTTL(*lock); err != nil {
		t.Fatalf("%v", err)
	}
	return lock
}

func createTable(t *testing.T, ddb *dynamodb.DynamoDB, tableName string) error {
	_, err := ddb.CreateTable(&dynamodb.CreateTableInput{
		TableName:        &tableName,
		SSESpecification: &dynamodb.SSESpecification{Enabled: aws.Bool(true)},
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("key"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("key"),
				KeyType:       aws.String("HASH"),
			},
		},
		BillingMode: aws.String("PAY_PER_REQUEST"),
	})

	if err != nil {
		return fmt.Errorf("creating table failed: %v", err)
	}
	t.Logf("submitted CreateTable")

	var tries int
	for tries = 0; tries < 3; tries++ {
		_, err := ddb.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: &tableName,
		})
		if err != nil {
			t.Logf("DescribeTable errored, waiting 10 seconds: %v", err)
			time.Sleep(10 * time.Second)
		} else {
			break
		}
	}
	if tries == 3 {
		return fmt.Errorf("table did not create")
	}
	return nil
}

func deleteTable(t *testing.T, ddb *dynamodb.DynamoDB, tableName string) error {
	_, err := ddb.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: &tableName,
	})

	if err != nil {
		return fmt.Errorf("deleting table failed: %v", err)
	}
	t.Logf("submitted DeleteTable")

	var tries int
	for tries = 0; tries < 3; tries++ {
		_, err := ddb.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: &tableName,
		})
		if err == nil {
			t.Logf("DescribeTable found the table, waiting 10 seconds: %v", err)
			time.Sleep(10 * time.Second)
		} else {
			break
		}
	}
	if tries == 3 {
		return fmt.Errorf("table did not delete")
	}
	return nil
}

func scanTable(ddb *dynamodb.DynamoDB, tableName string) (string, error) {
	scanOut, err := ddb.Scan(&dynamodb.ScanInput{
		TableName: &tableName,
	})

	if err != nil {
		return "", fmt.Errorf("scanning ntable failed: %v", err)
	}

	items := []string{}
	for _, item := range scanOut.Items {
		items = append(items, fmt.Sprintf("%v", item))
	}
	return strings.Join(items, "\n\n"), nil
}
