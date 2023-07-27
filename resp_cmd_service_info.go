package replica

import (
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/weedge/pkg/driver"
	standalone "github.com/weedge/xdis-standalone"
)

type RplStats struct {
	PubLogNum            atomic.Int64
	PubLogAckNum         atomic.Int64
	PubLogTotalAckTimeMs atomic.Int64

	MasterLastLogID atomic.Uint64
}

func (m *RplStats) StaticsPubLogTotalAckTime(handle func()) {
	startTime := time.Now()
	handle()
	m.PubLogAckNum.Add(1)
	m.PubLogTotalAckTimeMs.Add(time.Since(startTime).Milliseconds())
}

type SrvInfo struct {
	*standalone.SrvInfo

	srv      *RespCmdService
	rplStats *RplStats
}

func NewSrvInfo(srv *RespCmdService) (srvInfo *SrvInfo) {
	srvInfo = new(SrvInfo)
	srvInfo.srv = srv
	srvInfo.SrvInfo = standalone.NewSrvInfo(srv.RespCmdService)
	srvInfo.rplStats = &RplStats{}

	driver.RegisterDumpHandler("replication", srvInfo.DumpReplication)

	return
}

func (m *SrvInfo) DumpReplication(w io.Writer) {
	p := []driver.InfoPair{}

	m.srv.rplSlave.Lock()
	slaveof := m.srv.opts.ReplicaCfg.ReplicaOf
	m.srv.rplSlave.Unlock()

	masterLastLogId := m.rplStats.MasterLastLogID.Load()

	s, _ := m.srv.replica.Stat()
	isSlave := len(slaveof) > 0
	if !isSlave { //master
		p = append(p, driver.InfoPair{Key: "role", Value: "master"})

		length := len(m.srv.slaves)
		slaves := make([]string, 0, length)
		lastSynIds := make([]uint64, 0, length)
		closeds := make([]bool, 0, length)

		m.srv.slock.Lock()
		for _, s := range m.srv.slaves {
			slaves = append(slaves, s.slaveListeningAddr)
			lastSynIds = append(lastSynIds, s.lastLogID.Load())
			closeds = append(closeds, s.Closed())
		}
		m.srv.slock.Unlock()

		p = append(p, driver.InfoPair{Key: "connected_slaves", Value: length})
		for i := range slaves {
			host, port, _ := net.SplitHostPort(slaves[i])
			state := "online"
			if closeds[i] {
				state = "offline"
			}
			val := fmt.Sprintf(
				"ip=%s,port=%s,state=%s,last_sync_id=%d,lag=%d",
				host, port, state, lastSynIds[i], s.LastID-lastSynIds[i],
			)
			p = append(p, driver.InfoPair{Key: fmt.Sprintf("slave%d", i), Value: val})
		}
		//p = append(p, driver.InfoPair{Key: "slaves", Value: strings.Join(slaves, ",")})
	} else { // slave
		p = append(p, driver.InfoPair{Key: "role", Value: "slave"})

		// add some redis slave replication info for outer failover service :-)
		host, port, _ := net.SplitHostPort(slaveof)
		p = append(p, driver.InfoPair{Key: "master_host", Value: host})
		p = append(p, driver.InfoPair{Key: "master_port", Value: port})
		state := m.srv.rplSlave.state.Load()
		if state == RplSyncState || state == RplConnectedState {
			p = append(p, driver.InfoPair{Key: "master_link_status", Value: "up"})
		} else {
			p = append(p, driver.InfoPair{Key: "master_link_status", Value: "down"})
		}

		// here, all the slaves have same priority now
		p = append(p, driver.InfoPair{Key: "slave_priority", Value: DefaultSlavePriority})
		readOnly := 0
		if m.srv.opts.ReplicaCfg.GetReadonly() {
			readOnly = 1
		}
		p = append(p, driver.InfoPair{Key: "slave_read_only", Value: readOnly})

		if s != nil {
			if s.LastID > 0 {
				p = append(p, driver.InfoPair{Key: "slave_repl_offset", Value: s.LastID})
			} else {
				p = append(p, driver.InfoPair{Key: "slave_repl_offset", Value: s.CommitID})
			}
		} else {
			p = append(p, driver.InfoPair{Key: "slave_repl_offset", Value: 0})
		}
	}

	num := m.rplStats.PubLogNum.Load()
	p = append(p, driver.InfoPair{Key: "pub_log_num", Value: num})

	ackNum := m.rplStats.PubLogAckNum.Load()
	totalTime := m.rplStats.PubLogTotalAckTimeMs.Load()
	if ackNum != 0 {
		p = append(p, driver.InfoPair{Key: "pub_log_ack_per_time", Value: totalTime / ackNum})
	} else {
		p = append(p, driver.InfoPair{Key: "pub_log_ack_per_time", Value: 0})
	}

	if s != nil {
		p = append(p, driver.InfoPair{Key: "last_log_id", Value: s.LastID})
		p = append(p, driver.InfoPair{Key: "first_log_id", Value: s.FirstID})
		p = append(p, driver.InfoPair{Key: "commit_log_id", Value: s.CommitID})
	} else {
		p = append(p, driver.InfoPair{Key: "last_log_id", Value: 0})
		p = append(p, driver.InfoPair{Key: "first_log_id", Value: 0})
		p = append(p, driver.InfoPair{Key: "commit_log_id", Value: 0})
	}
	p = append(p, driver.InfoPair{Key: "master_last_log_id", Value: masterLastLogId})

	m.DumpPairs(w, p...)
}
