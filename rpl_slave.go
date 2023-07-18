package replica

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
	respclient "github.com/weedge/pkg/client/resp"
)

type ReplicaSlave struct {
	sync.Mutex
	srv *RespCmdService

	// masterAddr init by replicaof/salveof
	masterAddr string
	// state replication slave connect state
	state atomic.Int32
	// syncBuf used by slave sync log
	syncBuf bytes.Buffer

	// connLock use lock conn init/close
	connLock sync.Mutex
	// client is slave connect to master, init by sync
	// send internal cmd and recieve log/snapshot
	client *respclient.RespCmdClient

	// wait quit when slave close
	quitCh chan struct{}
	wg     sync.WaitGroup
}

func NewReplicaSlave(srv *RespCmdService) *ReplicaSlave {
	m := new(ReplicaSlave)
	m.srv = srv
	m.state.Store(RplConnectState)
	m.quitCh = make(chan struct{}, 1)

	return m
}

func (m *ReplicaSlave) Close() {
	m.state.Store(RplConnectState)
	if !m.isQuited() {
		close(m.quitCh)
	}

	m.closeConn()
	m.wg.Wait()
}

func (m *ReplicaSlave) closeConn() {
	m.connLock.Lock()
	defer m.connLock.Unlock()

	if m.client != nil {
		//for replication, when send quit command to close gracefully
		m.client.GetConn().SetReadDeadline(time.Now().Add(1 * time.Second))
		m.client.Close()
	}

	m.client = nil
}

func (m *ReplicaSlave) checkConn(ctx context.Context) error {
	m.connLock.Lock()
	defer m.connLock.Unlock()

	var err error
	if m.client == nil {
		m.client, err = respclient.Connect(m.masterAddr)

		if err != nil {
			return err
		}
	}

	// todo: check auth @weedge

	if _, err = m.client.DoWithStringArgs("PING"); err != nil {
		m.client.Close()
		m.client = nil
	}

	return err
}

func (m *ReplicaSlave) startReplication(ctx context.Context, masterAddr string, restart bool) error {
	//stop last replcation, if avaliable
	m.Close()

	m.masterAddr = masterAddr
	m.srv.replica.cfg.SetReadonly(true)
	m.quitCh = make(chan struct{}, 1)

	if len(m.masterAddr) == 0 {
		return fmt.Errorf("no assign ReplicaSlave addr")
	}

	m.wg.Add(1)
	go m.runReplication(ctx, restart)
	return nil
}

func (m *ReplicaSlave) isQuited() bool {
	select {
	case <-m.quitCh:
		return true
	default:
		return false
	}
}

func (m *ReplicaSlave) runReplication(ctx context.Context, restart bool) {
	defer func() {
		m.state.Store(RplConnectState)
		m.wg.Done()
	}()

	for {
		m.state.Store(RplConnectState)
		if m.isQuited() {
			return
		}

		// connect
		if err := m.checkConn(ctx); err != nil {
			klog.CtxErrorf(ctx, "check ReplicaSlave %s connection error %s, try 3s later", m.masterAddr, err.Error())

			select {
			case <-time.After(3 * time.Second):
			case <-m.quitCh:
				return
			}
			continue
		}

		if m.isQuited() {
			return
		}

		m.state.Store(RplConnectedState)
		// regisiter slave to ReplicaSlave
		if err := m.replConf(ctx); err != nil {
			if strings.Contains(err.Error(), ErrRplNotSupport.Error()) {
				klog.CtxErrorf(ctx, "ReplicaSlave doesn't support replication, wait 10s and retry")
				select {
				case <-time.After(10 * time.Second):
				case <-m.quitCh:
					return
				}
			} else {
				klog.CtxErrorf(ctx, "replconf error %s", err.Error())
			}

			continue
		}

		if restart {
			if err := m.fullSync(ctx); err != nil {
				klog.CtxErrorf(ctx, "restart fullsync error %s", err.Error())
				continue
			}
			m.state.Store(RplConnectedState)
		}

		// sync loop
		for {
			if err := m.sync(ctx); err != nil {
				klog.CtxErrorf(ctx, "sync error %s", err.Error())
				break
			}
			m.state.Store(RplConnectedState)

			if m.isQuited() {
				return
			}
		}
	}
}

func (m *ReplicaSlave) replConf(ctx context.Context) error {
	_, port, err := net.SplitHostPort(m.srv.opts.Addr)
	if err != nil {
		return err
	}

	if s, err := respclient.String(m.client.DoWithStringArgs("replconf", "listening-port", port)); err != nil {
		return err
	} else if strings.ToUpper(s) != "OK" {
		return fmt.Errorf("not ok but %s", s)
	}

	return nil
}

func (m *ReplicaSlave) fullSync(ctx context.Context) error {
	klog.CtxInfof(ctx, "begin full sync")

	if err := m.client.SendWithStringArgs("fullsync"); err != nil {
		return err
	}

	m.state.Store(RplSyncState)

	dumpPath := path.Join(m.srv.opts.ReplicaCfg.Path, "ReplicaSlave.dump")
	f, err := os.OpenFile(dumpPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	defer os.Remove(dumpPath)

	err = m.client.ReceiveBulkTo(f)
	f.Close()
	if err != nil {
		klog.CtxErrorf(ctx, "read dump data error %s", err.Error())
		return err
	}

	// loadDump clears all data and loads dump file to db
	if _, err = m.srv.replica.LoadDumpFile(ctx, dumpPath); err != nil {
		klog.CtxErrorf(ctx, "load dump file error %s", err.Error())
		return err
	}

	return nil
}

func (m *ReplicaSlave) nextSyncLogID() (uint64, error) {
	s, err := m.srv.replica.Stat()
	if err != nil {
		return 0, err
	}

	if s.LastID > s.CommitID {
		return s.LastID + 1, nil
	}
	return s.CommitID + 1, nil
}

func (m *ReplicaSlave) sync(ctx context.Context) error {
	var err error
	var syncID uint64
	if syncID, err = m.nextSyncLogID(); err != nil {
		return err
	}

	if err := m.client.Send("sync", syncID); err != nil {
		return err
	}

	m.state.Store(RplConnectedState)
	m.syncBuf.Reset()
	if err = m.client.ReceiveBulkTo(&m.syncBuf); err != nil {
		if strings.Contains(err.Error(), ErrLogMissed.Error()) {
			return m.fullSync(ctx)
		}
		return err
	}

	m.state.Store(RplConnectedState)
	buf := m.syncBuf.Bytes()
	if len(buf) < 8 {
		return fmt.Errorf("inavlid sync size %d", len(buf))
	}

	// todo: statics replica info

	buf = buf[8:]
	if len(buf) == 0 {
		return nil
	}

	if err = m.srv.replica.StoreLogsFromData(buf); err != nil {
		return err
	}

	return nil

}
