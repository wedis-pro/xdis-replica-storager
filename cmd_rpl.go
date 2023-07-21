package replica

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/tidwall/redcon"
	"github.com/weedge/pkg/driver"
	"github.com/weedge/pkg/utils"
	standalone "github.com/weedge/xdis-standalone"
)

func init() {
	driver.RegisterCmd(driver.CmdTypeReplica, "sync", syncCmd)
	driver.RegisterCmd(driver.CmdTypeReplica, "fullsync", fullsyncCmd)
	driver.RegisterCmd(driver.CmdTypeReplica, "replconf", replconfCmd)

	driver.RegisterCmd(driver.CmdTypeReplica, "replicaof", replicaofCommand)
	driver.RegisterCmd(driver.CmdTypeReplica, "slaveof", replicaofCommand)
	driver.RegisterCmd(driver.CmdTypeReplica, "role", roleCommand)
}

func getReplicaSrvRespConn(c driver.IRespConn) (*RespCmdConn, error) {
	respCmdConn, ok := c.(*RespCmdConn)
	if !ok || respCmdConn.srv == nil {
		return nil, ErrNoInitRespConn
	}
	if respCmdConn.srv.replica == nil {
		return nil, ErrRplNotSupport
	}
	return respCmdConn, nil
}

func splitHostPort(str string) (string, int16, error) {
	host, port, err := net.SplitHostPort(str)
	if err != nil {
		return "", 0, err
	}

	p, err := strconv.ParseInt(port, 10, 16)
	if err != nil {
		return "", 0, err
	}

	return host, int16(p), nil
}

func rplStateStr(r int32) string {
	switch r {
	case RplConnectState:
		return "connect"
	case RplConnectingState:
		return "connecting"
	case RplSyncState:
		return "sync"
	case RplConnectedState:
		return "connected"
	default:
		return "unknown"
	}
}

func roleCommand(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 0 {
		return nil, standalone.ErrCmdParams
	}

	respCmdConn, err := getReplicaSrvRespConn(c)
	if err != nil {
		return nil, err
	}

	respCmdConn.srv.rplSlave.Lock()
	replicaof := respCmdConn.srv.opts.ReplicaCfg.ReplicaOf
	respCmdConn.srv.rplSlave.Unlock()
	isMaster := len(replicaof) == 0

	data := make([]any, 0, 5)
	var lastID int64 = 0

	stat, _ := respCmdConn.srv.replica.Stat()
	if stat != nil {
		lastID = int64(stat.LastID)
	}
	if isMaster {
		data = append(data, "master")
		data = append(data, redcon.SimpleInt(lastID))

		items := make([]any, 0, 3)
		respCmdConn.srv.slock.Lock()
		for addr, slave := range respCmdConn.srv.slaves {
			host, port, _ := net.SplitHostPort(addr)
			items = append(items, []any{
				host,
				port,
				strconv.AppendUint(nil, slave.lastLogID.Load(), 10),
			})
		}
		respCmdConn.srv.slock.Unlock()
		data = append(data, items)
	} else {
		host, port, _ := splitHostPort(replicaof)
		data = append(data, "slave")
		data = append(data, host)
		data = append(data, redcon.SimpleInt(port))
		data = append(data, rplStateStr(respCmdConn.srv.rplSlave.state.Load()))
		data = append(data, redcon.SimpleInt(lastID))
	}
	res = data

	return
}

// client command, start to slaveof/replicaof master
func replicaofCommand(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 2 && len(cmdParams) != 3 {
		return nil, standalone.ErrCmdParams
	}

	masterAddr := ""
	restart := false
	readonly := false

	if strings.ToLower(utils.Bytes2String(cmdParams[0])) == "no" &&
		strings.ToLower(utils.Bytes2String(cmdParams[1])) == "one" {
		//stop replication, use master = ""
		if len(cmdParams) == 3 && strings.ToLower(utils.Bytes2String(cmdParams[2])) == "readonly" {
			readonly = true
		}
	} else {
		// port
		if _, err = strconv.ParseInt(utils.Bytes2String(cmdParams[1]), 10, 16); err != nil {
			return
		}

		masterAddr = fmt.Sprintf("%s:%s", cmdParams[0], cmdParams[1])
		if len(cmdParams) == 3 && strings.ToLower(utils.Bytes2String(cmdParams[2])) == "restart" {
			restart = true
		}
	}

	respCmdConn, err := getReplicaSrvRespConn(c)
	if err != nil {
		return nil, err
	}

	if err = respCmdConn.Replicaof(ctx, masterAddr, restart, readonly); err != nil {
		return
	}

	return standalone.OK, nil
}

// inner command, only for slave replication sync
func syncCmd(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) != 1 {
		return nil, standalone.ErrCmdParams
	}

	syncLogID, err := utils.StrUint64(cmdParams[0], nil)
	if err != nil {
		return nil, standalone.ErrCmdParams
	}

	respCmdConn, err := getReplicaSrvRespConn(c)
	if err != nil {
		return nil, err
	}

	buf, err := respCmdConn.Sync(syncLogID)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

// inner command, only for slave replication conf
// REPLCONF <option> <value> <option> <value> ...
// now only support "listening-port"
func replconfCmd(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams)%2 != 0 {
		return nil, standalone.ErrCmdParams
	}
	respCmdConn, err := getReplicaSrvRespConn(c)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(cmdParams); i += 2 {
		switch strings.ToLower(utils.Bytes2String(cmdParams[i])) {
		case "listening-port":
			var host string
			var err error
			if _, err = utils.StrUint64(cmdParams[i+1], err); err != nil {
				return nil, err
			}
			host, _, err = net.SplitHostPort(respCmdConn.GetRemoteAddr())
			if err != nil {
				return nil, err
			}
			respCmdConn.slaveListeningAddr = net.JoinHostPort(host, utils.Bytes2String(cmdParams[i+1]))

			respCmdConn.srv.addSlave(respCmdConn)
		default:
			return nil, standalone.ErrSyntax
		}
	}

	return standalone.OK, nil
}

// inner command, only for slave replication conf
// FULLSYNC <option> <new>
func fullsyncCmd(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	needNew := false
	if len(cmdParams) == 1 && strings.ToLower(utils.Bytes2String(cmdParams[0])) == "new" {
		needNew = true
	}

	respCmdConn, err := getReplicaSrvRespConn(c)
	if err != nil {
		return nil, err
	}
	respCmdConn.FullSync(ctx, needNew)

	return nil, standalone.ErrNoops
}
