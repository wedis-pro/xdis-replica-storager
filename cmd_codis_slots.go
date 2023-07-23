package replica

import (
	"bytes"
	"context"
	"hash/crc32"

	"github.com/tidwall/redcon"
	"github.com/weedge/pkg/driver"
	standalone "github.com/weedge/xdis-standalone"
)

// cmd more detail see: 
// https://github.com/CodisLabs/codis/blob/master/doc/redis_change_zh.md

func init() {
	driver.RegisterCmd(driver.CmdTypeReplica, "slothashkey", slotsHashKeyCmd)
}

// HashTag like redis cluster hash tag
func HashTag(key []byte) []byte {
	part := key
	if i := bytes.IndexByte(part, '{'); i != -1 {
		part = part[i+1:]
	} else {
		return key
	}
	if i := bytes.IndexByte(part, '}'); i != -1 {
		return part[:i]
	} else {
		return key
	}
}

func HashTagToSlot(tag []byte) uint32 {
	return crc32.ChecksumIEEE(tag) % MaxSlotNum
}

func HashKeyToSlot(key []byte) ([]byte, uint32) {
	tag := HashTag(key)
	return tag, HashTagToSlot(tag)
}

// SLOTSHASHKEY key [key...]
func slotsHashKeyCmd(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {
	if len(cmdParams) == 0 {
		return nil, standalone.ErrCmdParams
	}

	slots := make([]redcon.SimpleInt, 0, len(cmdParams))
	for _, key := range cmdParams {
		_, slot := HashKeyToSlot(key)
		slots = append(slots, redcon.SimpleInt(slot))
	}
	res = slots

	return
}

// SLOTSINFO [start] [count]
func slotsInfoCmd(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {

	return
}

// SLOTSMGRTONE host port timeout key
func slotsMgrtOneCmd(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {

	return
}

// SLOTSMGRTSLOT host port timeout slot
func slotsMgrtSlotCmd(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {

	return
}

// SLOTSMGRTTAGONE host port timeout key
func slotsMgrtTagOneCmd(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {

	return
}

// SLOTSMGRTTAGSLOT host port timeout slot
func slotsMgrtTagSlotCmd(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {

	return
}

// SLOTSRESTORE key ttlms value [key ttlms value ...]
func slotsRestoreCmd(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {

	return
}

// SLOTSCHECK
// for debug/test, don't use in product
func slotsCheckCmd(ctx context.Context, c driver.IRespConn, cmdParams [][]byte) (res interface{}, err error) {

	return
}

// SLOTSDEL
