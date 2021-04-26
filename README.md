# dynamodb-lock-go

`dynamodb-lock-go` is a library for using a DynamoDB table as a backend for distributed locking. A client is constructed by giving the name of the table which holds the locks. Then, the client methods use DynamoDB's conditional write so that only one owner can have the lock at a time. In order to guard against a process acquiring the lock and then crashing, locks have a limited lifetime called a lease. Once the lease time has expired, another owner will be able to claim the lock. A lease can be extended via the `HeartbeatLock` method.

The table schema needs to be set up with a attribute named `key` as the parition key as the only requirement. In addition, if the attribute `ttl` is set to be the TTL field for the table, then locks will be deleted 2 days after their last use.

Caveats:
* The user must ensure they provide different owner names; for example, a good owner name might be of the form "human-readable-owner-name:uuid for the specific job".
* Since locks expire and can be claimed by someone else, the user is responsible for checking that they still own the lock before using the resource.
* Since absolute times are stored in the table, the user is responsible for making sure the clocks on the machines are relatively in sync (there's a short grace period built in, but if a machine thinks the time is drastically different, things might go wrong).
* The table must be created ahead of time.

Owned by eng-infra
