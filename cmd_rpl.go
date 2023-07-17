package replica

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/weedge/pkg/driver"
	"github.com/weedge/pkg/utils"
	standalone "github.com/weedge/xdis-standalone"
)

func init() {
	driver.RegisterCmd(driver.CmdTypeReplica, "sync", syncCmd)
	driver.RegisterCmd(driver.CmdTypeReplica, "fullsync", fullsyncCommand)
	driver.RegisterCmd(driver.CmdTypeReplica, "replicaof", replicaofCommand)
	driver.RegisterCmd(driver.CmdTypeReplica, "replconf", replconfCmd)
}

func getReplicaSrvRespConn(c driver.IRespConn) (*RespCmdConn, error) {
	respCmdConn, ok := c.(*RespCmdConn)
	if !ok {
		return nil, ErrNoInitRespConn
	}
	if respCmdConn.srv.replica == nil {
		return nil, ErrRplNotSupport
	}
	return respCmdConn, nil
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

	if err = respCmdConn.Replicaof(masterAddr, restart, readonly); err != nil {
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
func fullsyncCommand(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	needNew := false
	if len(cmdParams) == 1 && strings.ToLower(utils.Bytes2String(cmdParams[0])) == "new" {
		needNew = true
	}

	respCmdConn, err := getReplicaSrvRespConn(c)
	if err != nil {
		return nil, err
	}
	respCmdConn.FullSync(needNew)

	return nil, standalone.ErrNoops
}
