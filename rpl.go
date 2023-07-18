package replica

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/golang/snappy"
	"github.com/weedge/pkg/driver"
	"github.com/weedge/pkg/safer"
	"github.com/weedge/pkg/utils"
	"github.com/weedge/xdis-replica-storager/config"
	storager "github.com/weedge/xdis-storager"
	"github.com/weedge/xdis-storager/openkv"
)

type Replication struct {
	// replica w/r op lock
	sync.Mutex

	// replica config
	cfg *config.ReplicationConfig

	// log store and purge expired log
	logStore IExpiredLogStore
	// commit log recode current commited logId
	commitLog *CommitLog

	// todo IStorager
	// storager to save kv data
	storager *storager.Storager

	// on replica replay ch, replay log to kvstore
	rc chan struct{}
	// kvstore WriteBatch for replay log save to kvstore from log store
	wbatch *openkv.WriteBatch

	// replica replay log save to kvstore has done ch, waits replication done
	rDoneCh chan struct{}

	// handlers which handle new log event
	rhs []NewLogEventHandler

	// no block notify ch for a new log has been stored to read
	// close(ch) and make ch no block pub, don't care sub
	nc chan struct{}
	// notify ch lock
	ncm sync.Mutex

	// close to wait goroutine job quitCh
	quitCh chan struct{}
	wg     sync.WaitGroup
}

// NewLogEventHandler is the handler to handle new log event.
type NewLogEventHandler func(rl *Log)

// AddNewLogEventHandler adds the handler for the new log event
func (r *Replication) AddNewLogEventHandler(h NewLogEventHandler) {
	r.rhs = append(r.rhs, h)
}

// propagate run event handle when has a new log event
func (r *Replication) propagateHandleAtNewLog(rl *Log) {
	for _, h := range r.rhs {
		h(rl)
	}
}

func NewReplication(cfg *config.ReplicationConfig) *Replication {
	rpl := &Replication{}
	rpl.initCfg(cfg)

	rpl.rc = make(chan struct{}, 1)
	rpl.rDoneCh = make(chan struct{}, 1)
	rpl.nc = make(chan struct{})
	rpl.quitCh = make(chan struct{})

	return rpl
}

func (r *Replication) initCfg(cfg *config.ReplicationConfig) {
	if len(cfg.Path) == 0 {
		cfg.Path = config.DefaultReplicaPath + cfg.ReplicaId
	}

	r.cfg = cfg
}

func (r *Replication) Close() error {
	close(r.quitCh)

	r.wg.Wait()

	r.Lock()
	defer r.Unlock()
	klog.Infof("closing replication with commit ID %d", r.commitLog.id)

	if r.logStore != nil {
		r.logStore.Close()
		r.logStore = nil
	}

	if err := r.commitLog.UpdateCommitID(r.commitLog.id, true); err != nil {
		klog.Errorf("update commit id err %s", err.Error())
	}

	if r.commitLog != nil {
		r.commitLog.Close()
		r.commitLog = nil
	}

	return nil
}

func (r *Replication) Start(ctx context.Context) (err error) {
	if r.logStore, err = GetExpiredLogStore(LogStoreName(r.cfg.StoreName)); err != nil {
		return
	}

	r.commitLog = new(CommitLog)
	if err = r.commitLog.InitCommitLog(r.cfg); err != nil {
		return
	}
	klog.Infof("staring replication with commit ID %d", r.commitLog.id)

	safer.GoSafely(&r.wg, false, r.runTickJob, nil, nil)
	safer.GoSafely(&r.wg, false, r.OnReplayLogToCommit, nil, nil)

	//first we must try wait all replication ok
	//maybe some logs are not committed
	if err = r.WaitReplication(); err != nil {
		return
	}

	return
}

func (r *Replication) runTickJob() {
	syncTc := time.NewTicker(1 * time.Second)
	purgeTc := time.NewTicker(1 * time.Hour)

	for {
		select {
		case <-purgeTc.C:
			n := (r.cfg.ExpiredLogDays * 24 * 3600)
			r.Lock()
			err := r.logStore.PurgeExpired(int64(n))
			r.Unlock()
			if err != nil {
				klog.Errorf("purge expired log error %s", err.Error())
			}
		case <-syncTc.C:
			if r.cfg.SyncLog == config.SyncLogEverySecond {
				r.Lock()
				err := r.logStore.Sync()
				r.Unlock()
				if err != nil {
					klog.Errorf("sync store error %s", err.Error())
				}
			}
			if r.cfg.SyncLog != config.SyncLogEveryCommit {
				//we will sync commit id every 1 second
				r.Lock()
				err := r.commitLog.UpdateCommitID(r.commitLog.id, true)
				r.Unlock()

				if err != nil {
					klog.Errorf("sync commitid error %s", err.Error())
				}
			}
		case <-r.quitCh:
			syncTc.Stop()
			purgeTc.Stop()
			return
		}
	}
}

func (r *Replication) SetStorager(store driver.IStorager) {
	r.storager = store.(*storager.Storager)
	r.wbatch = r.storager.GetKVStore().NewWriteBatch()
	r.storager.SetCommitter(r)
}

func (r *Replication) OnReplayLogToCommit() {
	r.noticeReplication()

	for {
		select {
		case <-r.rc:
			r.replayLogToCommit()
		case <-r.quitCh:
			return
		}
	}
}

func (r *Replication) noticeReplication() {
	utils.AsyncNoBlockSend(r.rc)
}

// replayLog rw lock until store log all commited save to kvstore,and update commit log
func (r *Replication) replayLogToCommit() (err error) {
	wrLock := r.storager.GetRWLock()
	wrLock.Lock()
	defer wrLock.Unlock()

	defer utils.AsyncNoBlockSend(r.rDoneCh)

	rl := &Log{}
	for {
		// get next commitId log from log store,(current commitId in commited.log)
		if err = r.NextNeedCommitLog(rl); err != nil {
			if err != ErrNoBehindLog {
				klog.Errorf("get next commit log err, %s", err.Error())
				return err
			}

			return nil
		}

		// reset clear WriteBatch data
		r.wbatch.Rollback()

		if rl.Compression == 1 {
			//todo optimize
			// compress decode log store Data
			if rl.Data, err = snappy.Decode(nil, rl.Data); err != nil {
				klog.Errorf("decode log error %s", err.Error())
				return err
			}
		}

		// log store Data([]byte) new Batch replay to WriteBatch for commit
		if bd, err := openkv.NewBatchData(rl.Data); err != nil {
			klog.Errorf("decode batch log error %s", err.Error())
			return err
		} else if err = bd.Replay(r.wbatch); err != nil {
			klog.Errorf("replay batch log error %s", err.Error())
		}

		// lock to WriteBatch commit and update commitId(logId) to commited.log
		r.storager.GetCommitLock().Lock()
		if err = r.wbatch.Commit(); err != nil {
			klog.Errorf("commit log error %s", err.Error())
		} else if err = r.UpdateCommitID(rl.ID); err != nil {
			klog.Errorf("update commit id error %s", err.Error())
		}
		r.storager.GetCommitLock().Unlock()

		if err != nil {
			return err
		}
	}
}

// NextNeedCommitLog get next commitId log from log store,(current commitId in commited.log)
func (r *Replication) NextNeedCommitLog(log *Log) error {
	r.Lock()
	defer r.Unlock()

	id, err := r.logStore.LastID()
	if err != nil {
		return err
	}

	if id <= r.commitLog.id {
		return ErrNoBehindLog
	}

	return r.logStore.GetLog(r.commitLog.id+1, log)
}

// UpdateCommitID update commitId(logId) to commited.log
func (r *Replication) UpdateCommitID(id uint64) error {
	r.Lock()
	err := r.commitLog.UpdateCommitID(id, r.cfg.SyncLog == config.SyncLogEveryCommit)
	r.Unlock()

	return err
}

// WaitReplication waits replication done when start init replica
func (r *Replication) WaitReplication() error {
	for i := 0; i < 100; i++ {
		r.noticeReplication()

		select {
		case <-r.rDoneCh:
		case <-r.quitCh:
			return nil
		}
		time.Sleep(100 * time.Millisecond)

		b, err := r.CommitIDBehind()
		if err != nil {
			return err
		} else if !b {
			return nil
		}
	}

	return errors.New("wait replication too many times")
}

// CommitIDBehind check commitId behind last logId
func (r *Replication) CommitIDBehind() (bool, error) {
	r.Lock()
	defer r.Unlock()

	id, err := r.logStore.LastID()
	if err != nil {
		return false, err
	}

	return id > r.commitLog.id, nil
}

// Commit  with WriteBatch for ICommitter Impl, when data batch atomic commit
func (r *Replication) Commit(ctx context.Context, wb *openkv.WriteBatch) (err error) {
	// check config only read
	if r.cfg.GetReadonly() {
		return ErrWriteInROnly
	}

	r.storager.GetCommitLock().Lock()
	defer r.storager.GetCommitLock().Unlock()

	var rl *Log
	// save kvstore data to log store, writebatch ---batchdata (raw data)---> log store
	if rl, err = r.LogStore(wb.Data()); err != nil {
		klog.Errorf("write wal error %s", err.Error())
		return err
	}
	r.PubNewLogNotify()

	// publishNewLog, wait sync quorum slaves pull log ack
	r.propagateHandleAtNewLog(rl)

	// commits or updates CommitID error,
	// master will block lock wirte, only read until replay goroutine (runtime schedule thread) executes this log correctly.
	if err = wb.Commit(); err != nil {
		klog.Errorf("commit error %s", err.Error())
		r.noticeReplication()
		return err
	}

	if err = r.UpdateCommitID(rl.ID); err != nil {
		klog.Errorf("update commit id error %s", err.Error())
		r.noticeReplication()
		return err
	}

	return err
}

// Log store log and notify wait
func (r *Replication) LogStore(data []byte) (*Log, error) {
	if r.cfg.Compression {
		// maybe use zstd
		data = snappy.Encode(nil, data)
	}

	r.Lock()
	defer r.Unlock()

	lastID, err := r.logStore.LastID()
	if err != nil {
		return nil, err
	}

	commitID := r.commitLog.id
	if lastID < commitID {
		lastID = commitID
	} else if lastID > commitID {
		return nil, ErrCommitIDBehind
	}

	l := new(Log)
	l.ID = lastID + 1
	l.CreateTime = uint32(time.Now().Unix())

	if r.cfg.Compression {
		l.Compression = 1
	} else {
		l.Compression = 0
	}

	l.Data = data

	if err = r.logStore.StoreLog(l); err != nil {
		return nil, err
	}

	return l, nil
}

// PubNewLogNotify pub new log has saved notify
func (r *Replication) PubNewLogNotify() {
	r.ncm.Lock()
	close(r.nc)
	r.nc = make(chan struct{})
	r.ncm.Unlock()
}

// WaitNewLog return sub new log has saved notify ch
func (r *Replication) WaitNewLog() <-chan struct{} {
	r.ncm.Lock()
	ch := r.nc
	r.ncm.Unlock()
	return ch
}

// ReadLogsToTimeout tries to read events, if no events read,
// wait a new log has been stored to read until timeout or processor quit
// just for sync log to slave
func (r *Replication) ReadLogsToTimeout(startLogID uint64, w io.Writer, timeout time.Duration) (n int, nextLogID uint64, err error) {
	n, nextLogID, err = r.ReadLogsTo(startLogID, w)
	if err != nil {
		return
	} else if n != 0 {
		return
	}

	select {
	// wait a new log has been stored to read
	case <-r.WaitNewLog():
	// read time out
	case <-time.After(timeout):
	// for master processor terminate to return current resp rsp
	case <-r.quitCh:
		return
	}

	return r.ReadLogsTo(startLogID, w)
}

// ReadLogsTo reads [startLogID, lastLogID] logs and write to the Writer.
// return total logs size n, n must <= MaxReplLogSize
// nextLogId next logId to sync
// if startLogID < logStore firtId, return ErrLogMissed, slave receive ErrLogMissed need full sync
func (r *Replication) ReadLogsTo(startLogID uint64, w io.Writer) (n int, nextLogID uint64, err error) {
	firtID, err := r.FirstLogID()
	if err != nil {
		return
	}

	// slave receive ErrLogMissed need full sync
	if startLogID < firtID {
		err = ErrLogMissed
		return
	}

	lastID, err := r.LastLogID()
	if err != nil {
		return
	}

	nextLogID = startLogID
	log := &Log{}
	for i := startLogID; i <= lastID; i++ {
		if err = r.logStore.GetLog(i, log); err != nil {
			return
		}

		if err = log.Encode(w); err != nil {
			return
		}

		nextLogID = i + 1
		n += log.Size()
		if n > MaxReplLogSize {
			break
		}
	}

	return
}

// StoreLogsFromReader stores logs from the Reader
func (r *Replication) StoreLogsFromReader(rb io.Reader) error {
	if !r.cfg.Readonly {
		return ErrRplInRDWR
	}

	log := &Log{}
	for {
		if err := log.Decode(rb); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if err := r.StoreLog(log); err != nil {
			return err
		}
	}

	r.noticeReplication()
	return nil
}

// StoreLogsFromData stores logs from data.
func (r *Replication) StoreLogsFromData(data []byte) error {
	rb := bytes.NewReader(data)
	return r.StoreLogsFromReader(rb)
}
