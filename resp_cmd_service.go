package replica

import (
	"context"
	"sync"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/weedge/pkg/driver"
	"github.com/weedge/pkg/utils"
	"github.com/weedge/xdis-replica-storager/config"
	standalone "github.com/weedge/xdis-standalone"
)

type RespCmdService struct {
	opts *config.RespCmdServiceOptions

	// standalone RespCmdService
	*standalone.RespCmdService

	// snapshot
	snapshotStore *SnapshotStore
	// replica
	replica *Replication
	// replica slave
	rplSlave *ReplicaSlave

	// manage slave connect
	slock        sync.Mutex
	slaves       map[string]*RespCmdConn
	slaveSyncAck chan uint64
}

func New(opts *config.RespCmdServiceOptions) *RespCmdService {
	if opts == nil {
		return nil
	}

	s := new(RespCmdService)
	s.opts = opts
	s.RespCmdService = standalone.New(&opts.RespCmdServiceOptions)
	s.snapshotStore = NewSnapshotStore(&opts.SnapshotCfg)
	s.replica = NewReplication(&opts.ReplicaCfg)

	s.slaves = make(map[string]*RespCmdConn)
	s.slaveSyncAck = make(chan uint64)

	return s
}

// Start service
func (s *RespCmdService) Start(ctx context.Context) (err error) {
	s.RespCmdService.Start(ctx)
	s.replica.AddNewLogEventHandler(s.publishNewLog)

	return
}

// Close resp service
func (s *RespCmdService) Close() (err error) {
	s.RespCmdService.Close()

	return
}

// Name
func (s *RespCmdService) Name() driver.RespServiceName {
	return config.RegisterRespSrvModeName
}

// SetStorager
func (s *RespCmdService) SetStorager(store driver.IStorager) {
	s.RespCmdService.SetStorager(store)
	s.replica.SetStorager(store)
}

// --- slave ---

func (s *RespCmdService) addSlave(c *RespCmdConn) {
	s.slock.Lock()
	s.slaves[c.slaveListeningAddr] = c
	s.slock.Unlock()
}

func (s *RespCmdService) removeSlave(c *RespCmdConn, activeQuit bool) {
	addr := c.slaveListeningAddr

	s.slock.Lock()
	defer s.slock.Unlock()

	if _, ok := s.slaves[addr]; ok {
		klog.Infof("remove slave %s", addr)
		delete(s.slaves, addr)
		utils.AsyncNoBlockSendUint64(s.slaveSyncAck, c.lastLogID.Load())
	}
}

// slaveAck slave sync log ok, notify a new log which waiting to commit
func (s *RespCmdService) slaveAck(c *RespCmdConn) {
	s.slock.Lock()
	defer s.slock.Unlock()

	if _, ok := s.slaves[c.slaveListeningAddr]; !ok {
		return
	}

	utils.AsyncNoBlockSendUint64(s.slaveSyncAck, c.lastLogID.Load())
}

// publishNewLog if config sync, wait sync Log from quorum slaves when log commit
// else return do async,don't wait
func (s *RespCmdService) publishNewLog(l *Log) {
	if !s.opts.ReplicaCfg.Sync {
		return
	}

	s.slock.Lock()
	slaveNum := len(s.slaves)
	total := (slaveNum + 1) / 2
	if s.opts.ReplicaCfg.WaitMaxSlaveAcks > 0 {
		total = utils.MinInt(total, s.opts.ReplicaCfg.WaitMaxSlaveAcks)
	}

	n := 0
	logID := l.ID
	for _, slave := range s.slaves {
		lastLogID := slave.lastLogID.Load()
		if lastLogID == logID {
			// slave has already owned this log
			n++
		} else if lastLogID > logID {
			klog.Errorf("invalid slave %s, lastlogid %d > %d", slave.slaveListeningAddr, lastLogID, logID)
		}
	}

	s.slock.Unlock()

	if n >= total {
		//at least total slaves have owned this log
		return
	}

	done := make(chan struct{}, 1)
	go func() {
		n := 0
		for i := 0; i < slaveNum; i++ {
			id := <-s.slaveSyncAck
			if id < logID {
				klog.Infof("some slave may close with last logid %d < %d", id, logID)
			} else {
				n++
				if n >= total {
					break
				}
			}
		}
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-time.After(time.Duration(s.opts.ReplicaCfg.WaitSyncTime) * time.Millisecond):
		klog.Info("replication wait timeout")
	}
}

// replicaof
func (s *RespCmdService) replicaof(masterAddr string, restart bool, readonly bool) error {
	s.rplSlave.Lock()
	defer s.rplSlave.Unlock()

	//in repilcaof/slaveof no one mode and config no replicaof, only set readonly
	if len(s.opts.ReplicaCfg.ReplicaOf) == 0 && len(masterAddr) == 0 {
		s.opts.ReplicaCfg.SetReadonly(readonly)
		return nil
	}

	s.opts.ReplicaCfg.ReplicaOf = masterAddr

	if len(masterAddr) == 0 {
		klog.Infof("replicaof no one, stop replication")
		s.rplSlave.Close()
		s.opts.ReplicaCfg.SetReadonly(readonly)
		return nil
	}

	return s.rplSlave.startReplication(masterAddr, restart)
}
