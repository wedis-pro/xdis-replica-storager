package config

import (
	"sync"

	standaloneCfg "github.com/weedge/xdis-standalone/config"
)

type RespCmdServiceOptions struct {
	standaloneCfg.RespCmdServiceOptions
	ReplicaCfg  ReplicationConfig `mapstructure:"replicaCfg"`
	SnapshotCfg SnapshotConfig    `mapstructure:"snapshotCfg"`
}

type ReplicationConfig struct {
	sync.RWMutex
	ReplicaOf        string `mapstructure:"replicaof"`
	ReplicaId        string `mapstructure:"replicaId"`
	Path             string `mapstructure:"path"`
	Sync             bool   `mapstructure:"sync"`
	WaitSyncTime     int    `mapstructure:"waitSyncTime"`
	WaitMaxSlaveAcks int    `mapstructure:"waitMaxSlaveAcks"`
	ExpiredLogDays   int    `mapstructure:"expiredLogDays"`
	StoreName        string `mapstructure:"storeName"`
	MaxLogFileSize   int64  `mapstructure:"maxLogFileSize"`
	MaxLogFileNum    int    `mapstructure:"maxLogFileNum"`
	SyncLog          int    `mapstructure:"syncLog"`
	Compression      bool   `mapstructure:"compression"`
	Readonly         bool   `mapstructure:"readonly"`
}

type SnapshotConfig struct {
	Path   string `mapstructure:"path"`
	MaxNum int    `mapstructure:"maxNum"`
}

func DefaultSnapshotConfig() *SnapshotConfig {
	return &SnapshotConfig{
		Path:   DefaultSnapshotPath,
		MaxNum: DefaultSnapshotMaxNum,
	}
}

func DefaultReplicationConfig() *ReplicationConfig {
	return &ReplicationConfig{
		Path: DefaultReplicaPath,
	}
}

func DefaultRespCmdServiceOptions() *RespCmdServiceOptions {
	return &RespCmdServiceOptions{
		ReplicaCfg:  *DefaultReplicationConfig(),
		SnapshotCfg: *DefaultSnapshotConfig(),
	}
}

func (cfg *ReplicationConfig) GetReadonly() bool {
	cfg.RLock()
	b := cfg.Readonly
	cfg.RUnlock()
	return b
}

func (cfg *ReplicationConfig) SetReadonly(b bool) {
	cfg.Lock()
	cfg.Readonly = b
	cfg.Unlock()
}
