# M/S replica
like redis/mysql Master/Slaver replica mode, just simple classic impl (no learner to start, no linear read, slave replica pull bluk RESP with binlog[startLogID,LastLogID]/snapshot file from master);
1. start with replicaof or exec `replicaof` cmd
   1. when start service, latest current commit log load to commitID
   2. then slave send `replicaaof` cmd to start connect master; between replica(master/slave), master start replica goroutine, create connect
   3. slave send `replconf` to master register slaves
   4. if use restart slave send `fullsync` cmd to master, master full sync from snapshot compress file to slave, more detail see `3`
   5. then start sync loop, send `sync/psync` cmd with slave current latest logId(syncId), 
   6. if slave's logId is less than master's firstLogId, master will tell slave log has been purged, the slave must do a full sync , more detail see `3`
   7. else master send [lastLogID+binlog] from log store to slave, util send ack[lastLogID] sync ok

2. For master, RESP `w` op cmd commit to save log, wait quorum slaves to ack(sync pull binlog ok), writeBatch to atomic commit to data kvstore, save commitID(logID) to latest current commit log; if save log OK but writeBatch atomic commit or update commitId error, it will also lock write until replication goroutine (runtime schedule thread) executes this log correctly. replay log below:
   1. get next commitId log from log store,(current commitId in commitedId.log)
   2. reset clear WriteBatch data, compress decode log store Data
   3. log store Data([]byte) new Batch replay to WriteBatch for commit
   4. lock to WriteBatch commit and update commitId(logId) to commited.log


3. slave send `fullsync` cmd to master
   1. master if don't exists snapshot file, or snapshot file has expired , create new snapshot (one connect per goroutine)
   2. if not, use lastest snapshot file (init ticker job to purge expired snapshot)
   3. then lock snapshot, create new snapshot use data kvstore (FSM) lock write to gen snapshot and iter it save to snapshot file (format: [len(compress key) | compress key | len(compress value) | compress value ...])
   4. from snapshot file read snapshot send bluk([]byte) RESP to slave
   5. slave receive the bluk RESP to save the dump file(reply log)
   6. then lock write to load, clear all data and load dump file write(put) to data kvstore (FSM)  

keyword:
+ LogID: a monotonically increasing integer for a log
+ FirstLogID: the oldest log id for a server, all the logs before this id have been purged.
+ LastLogID: the newest log id for a server.
+ CommitID: the last log committed to execute. If LastLogID is 10 and CommitID is 5, server needs to commit logs from 6 - 10 to catch the up to date status.

slave replica connect state:
* RplConnectState: slave needs to connect to its master
* RplConnectingState: slave-master connection is in progress
* RplSyncState: perform the synchronization
* RplConnectedState: slave is online

file:
* `commit.log`: record current committed logId which has saved to the log store
* store log file: replica store log (ILogStore impl eg: WAL)
* snapshot file: replica snapshot file for fullsync, format [len(compress key) | compress key | len(compress value) | compress value ...]

Notice:
* if master down, need HA failover server to select a new master role; then slave slaveof/replicaof master to sync log.
* if store node auto HA failover, need some transport collaboration protocol to select a new leader like raft/paxos consistency protocol, then leader sync log to followers

# reference
* [ledisdb](https://github.com/ledisdb/ledisdb)