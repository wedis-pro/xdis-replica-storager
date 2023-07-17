package replica

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"

	standalone "github.com/weedge/xdis-standalone"
)

type RespCmdConn struct {
	*standalone.RespCmdConn

	srv *RespCmdService
	// lastLogID record for slave's connect to sync log
	lastLogID atomic.Uint64
	// slaveListeningAddr slave listening addr for send sync log
	slaveListeningAddr string
	// syncBuf is used to buff one slave's connect to sync log
	// bytes.Buffer dynamic grow
	syncBuf bytes.Buffer
}

// Sync
func (c *RespCmdConn) Sync(syncLogID uint64) (buf []byte, err error) {
	lastLogID := syncLogID - 1
	stat, err := c.srv.replica.Stat()
	if err != nil {
		return nil, err
	}

	if lastLogID > stat.LastID {
		return nil, fmt.Errorf("invalid sync logid %d > %d + 1", syncLogID, stat.LastID)
	}

	c.lastLogID.Store(lastLogID)

	if lastLogID == stat.LastID {
		c.srv.slaveAck(c)
	}

	// reset dynamic grow sync bytes.buff, write empty dummy buff [8]byte[:] for uint64 last logId
	c.syncBuf.Reset()
	c.syncBuf.Write(make([]byte, 8))

	if _, _, err = c.srv.replica.ReadLogsToTimeout(syncLogID, &c.syncBuf, 1*time.Second); err != nil {
		return
	}

	stat, err = c.srv.replica.Stat()
	if err != nil {
		return nil, err
	}

	buf = c.syncBuf.Bytes()
	binary.BigEndian.PutUint64(buf, stat.LastID)

	return
}

// Replicaof
func (c *RespCmdConn) Replicaof(masterAddr string, restart bool, readonly bool) (err error) {
	err = c.srv.replicaof(masterAddr, restart, readonly)
	return
}

// FullSync
func (c *RespCmdConn) FullSync(needNew bool) (err error) {
	var s *snapshot
	var t time.Time

	dumper := c.srv.replica
	if needNew {
		s, _, err = c.srv.snapshotStore.Create(dumper)
	} else {
		if s, t, err = c.srv.snapshotStore.OpenLatest(); err != nil {
			return err
		} else if s == nil {
			s, _, err = c.srv.snapshotStore.Create(dumper)
		} else {
			gap := time.Duration(c.srv.replica.cfg.ExpiredLogDays*24*3600) * time.Second / 2
			minT := time.Now().Add(-gap)
			//snapshot is too old
			if t.Before(minT) {
				s.Close()
				s, _, err = c.srv.snapshotStore.Create(dumper)
			}
		}
	}
	if err != nil {
		return err
	}

	c.WriteBulkFrom(s.Size(), s)
	s.Close()

	return
}
