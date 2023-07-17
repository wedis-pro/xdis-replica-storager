package config

const (
	RegisterRespSrvModeName = "replica-master-slave"
)

// defualt config
const (
	DefaultReplicaPath = "./data/rpl"

	DefaultSnapshotPath   = "./data/snapshot"
	DefaultSnapshotMaxNum = 1
)

// Sync log to disk
//
//	0: no sync
//	1: sync every second
//	2: sync every commit
const (
	SyncLogNo          = 0
	SyncLogEverySecond = 1
	SyncLogEveryCommit = 2
)
