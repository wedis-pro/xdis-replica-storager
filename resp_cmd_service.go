package replica

import (
	"context"
	"sync"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/tidwall/redcon"
	"github.com/weedge/pkg/driver"
	"github.com/weedge/pkg/utils"
	"github.com/weedge/xdis-replica-storager/config"
	standalone "github.com/weedge/xdis-standalone"
	standaloneCfg "github.com/weedge/xdis-standalone/config"
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

	//rplSrvInfo repliaction info
	rplSrvInfo *SrvInfo
}

var (
	gReplicaId string
)

func New(opts *config.RespCmdServiceOptions, standaloneOpts *standaloneCfg.RespCmdServiceOptions) *RespCmdService {
	if opts == nil {
		return nil
	}

	s := new(RespCmdService)
	s.opts = opts
	s.opts.RespCmdServiceOptions = *standaloneOpts

	s.RespCmdService = standalone.New(&opts.RespCmdServiceOptions)
	s.replica = NewReplication(&opts.ReplicaCfg)
	gReplicaId = opts.ReplicaCfg.ReplicaId
	s.snapshotStore = NewSnapshotStore(&opts.SnapshotCfg)

	s.rplSlave = NewReplicaSlave(s)

	s.slaves = make(map[string]*RespCmdConn)
	s.slaveSyncAck = make(chan uint64)

	return s
}

func (s *RespCmdService) RegisterLogStore() error {
	logStore := NewOpenkvLogStore(&s.opts.LogStoreOpenkvCfg)
	if err := RegisterExpiredLogStore(logStore); err != nil {
		return err
	}
	return nil
}

func (s *RespCmdService) InitRespConn(ctx context.Context, dbIdx int) driver.IRespConn {
	c := s.RespCmdService.InitRespConn(ctx, dbIdx)
	conn := &RespCmdConn{
		RespCmdConn: c.(*standalone.RespCmdConn),
		srv:         s,
	}

	return conn
}

func (s *RespCmdService) OnAccept(conn redcon.Conn) bool {
	klog.Infof("accept: %s", conn.RemoteAddr())

	// todo: get net.Conn request info set to context Value for trace
	// add resp cmd conn
	respConn := s.InitRespConn(context.Background(), 0)
	respCmdConn := respConn.(*RespCmdConn)
	respCmdConn.SetRedConn(conn)
	respCmdConn.SetStorager(s.replica.storager)
	s.AddRespCmdConn(respCmdConn)

	// set ctx
	conn.SetContext(respCmdConn)
	return true
}

func (s *RespCmdService) OnClosed(conn redcon.Conn, err error) {
	logF := klog.Infof
	if err != nil {
		logF = klog.Errorf
	}
	logF("closed by %s, err: %v", conn.RemoteAddr(), err)

	// del resp cmd conn
	respCmdConn, ok := conn.Context().(*RespCmdConn)
	if !ok {
		klog.Errorf("resp cmd connect client init err")
		return
	}

	if s.RespCmdService != nil {
		s.RespCmdService.DelRespCmdConn(respCmdConn)
	}

	s.removeSlave(respCmdConn)
}

// Start service set onAccept onClosed then start resp cmd service
func (s *RespCmdService) Start(ctx context.Context) (err error) {
	s.rplSrvInfo = NewSrvInfo(s)
	s.SetSrvInfo(s.rplSrvInfo)
	s.SetOnAccept(s.OnAccept)
	s.SetOnClosed(s.OnClosed)
	driver.MergeRegisteredCmdHandles(driver.RegisteredCmdHandles, driver.RegisteredReplicaCmdHandles, true)
	s.SetRegisteredCmdHandles(driver.RegisteredReplicaCmdHandles)

	s.replica.AddNewLogEventHandler(s.publishNewLog)
	if err = s.RegisterLogStore(); err != nil {
		return
	}
	if err = s.replica.Start(ctx); err != nil {
		return
	}

	if err = s.snapshotStore.Open(ctx); err != nil {
		return
	}

	if len(s.opts.ReplicaCfg.ReplicaOf) > 0 {
		s.replicaof(ctx, s.opts.ReplicaCfg.ReplicaOf, false, s.opts.ReplicaCfg.Readonly)
	}

	// info repliation need logstore open, so need finally start service
	if err = s.RespCmdService.Start(ctx); err != nil {
		return
	}

	return
}

// Close resp service
func (s *RespCmdService) Close() (err error) {
	if s.rplSlave != nil {
		s.rplSlave.Close()
		s.rplSlave = nil
	}

	if s.RespCmdService != nil {
		s.RespCmdService.Close()
		s.RespCmdService = nil
	}

	if s.replica != nil {
		s.replica.Close()
		s.replica = nil
	}

	if s.snapshotStore != nil {
		s.snapshotStore.Close()
		s.snapshotStore = nil
	}

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

func (s *RespCmdService) removeSlave(c *RespCmdConn) {
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

	s.rplSrvInfo.rplStats.PubLogNum.Add(1)
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

	s.rplSrvInfo.rplStats.StaticsPubLogTotalAckTime(func() {
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
	})
}

// replicaof
func (s *RespCmdService) replicaof(ctx context.Context, masterAddr string, restart bool, readonly bool) error {
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

	return s.rplSlave.startReplication(ctx, masterAddr, restart)
}
