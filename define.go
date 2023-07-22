package replica

import "errors"

const (
	InvalidLogID uint64 = 0

	MaxReplLogSize = 1 * 1024 * 1024

	DefaultSlavePriority = 100
)

const (
	// slave needs to connect to its master
	RplConnectState int32 = iota + 1
	// slave-master connection is in progress
	RplConnectingState
	// perform the synchronization
	RplSyncState
	// slave is online
	RplConnectedState
)

var (
	ErrLogNotFound    = errors.New("log not found")
	ErrLogMissed      = errors.New("log is pured in server")
	ErrStoreLogID     = errors.New("log id is less")
	ErrNoBehindLog    = errors.New("no behind commit log")
	ErrCommitIDBehind = errors.New("commit id is behind last log id")

	ErrWriteInROnly  = errors.New("write not support in readonly mode")
	ErrRplInRDWR     = errors.New("replication not support in read write mode")
	ErrRplNotSupport = errors.New("replication not support")
)
