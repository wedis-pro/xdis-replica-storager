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
as the same redis role: https://redis.io/commands/role/

The state of the replication from the point of view of the master, that can be *connect* (the instance needs to connect to its master), *connecting* (the master-replica connection is in progress), *sync* (the master and replica are trying to perform the synchronization), *connected* (the replica is online).

* RplConnectState: slave needs to connect to its master
* RplConnectingState: slave-master connection is in progress
* RplSyncState: perform the synchronization
* RplConnectedState: slave is online

file:
* `commit.log`: record current committed logId which has saved to the log store
* store log file: replica store log (ILogStore impl eg: WAL)
* snapshot file: replica snapshot file for fullsync, format [len(compress key) | compress key | len(compress value) | compress value ...]

Notice (keep HA, need a HA failover mechanism, majority select master):
* if master down, need HA failover server to select a new master role; then slave slaveof/replicaof master to sync log.
* if store node auto HA failover, need some transport collaboration protocol to select a new leader like raft/paxos consistency protocol, then leader sync log to followers, or like redsi cluster other majority masters use gossip protocol to select a master and notify other masters exchange meta message. 
* a new master seleted (in term/epoch) has done, then notify proxy(codis) to promote

so before do something, need think alternative (failover). `Don't put all your eggs in one basket`

# feature
1. support redis sentinel to keep M/S replica HA failover
   1. add pub/sub for `__sentinel__:hello` channel
   2. support `info` cmd, add `Replication` section

redis-sentinel log:
```
40717:X 22 Jul 2023 18:48:15.683 # Sentinel ID is ace6225b2a8faddaf6ad599a8db8b504e0ec2b9d
40717:X 22 Jul 2023 18:48:15.683 # +monitor master mymaster 127.0.0.1 6666 quorum 1
40717:X 22 Jul 2023 18:48:15.684 * +slave slave 127.0.0.1:6667 127.0.0.1 6667 @ mymaster 127.0.0.1 6666


40717:X 22 Jul 2023 19:03:34.694 # +sdown master mymaster 127.0.0.1 6666
40717:X 22 Jul 2023 19:03:34.694 # +odown master mymaster 127.0.0.1 6666 #quorum 1/1
40717:X 22 Jul 2023 19:03:34.697 # +new-epoch 9
40717:X 22 Jul 2023 19:03:34.697 # +try-failover master mymaster 127.0.0.1 6666
40717:X 22 Jul 2023 19:03:34.702 # +vote-for-leader ace6225b2a8faddaf6ad599a8db8b504e0ec2b9d 9
40717:X 22 Jul 2023 19:03:34.702 # +elected-leader master mymaster 127.0.0.1 6666
40717:X 22 Jul 2023 19:03:34.702 # +failover-state-select-slave master mymaster 127.0.0.1 6666
40717:X 22 Jul 2023 19:03:34.761 # +selected-slave slave 127.0.0.1:6667 127.0.0.1 6667 @ mymaster 127.0.0.1 6666
40717:X 22 Jul 2023 19:03:34.761 * +failover-state-send-slaveof-noone slave 127.0.0.1:6667 127.0.0.1 6667 @ mymaster 127.0.0.1 6666
40717:X 22 Jul 2023 19:03:34.852 * +failover-state-wait-promotion slave 127.0.0.1:6667 127.0.0.1 6667 @ mymaster 127.0.0.1 6666
40717:X 22 Jul 2023 19:03:35.712 # +promoted-slave slave 127.0.0.1:6667 127.0.0.1 6667 @ mymaster 127.0.0.1 6666
40717:X 22 Jul 2023 19:03:35.712 # +failover-state-reconf-slaves master mymaster 127.0.0.1 6666
40717:X 22 Jul 2023 19:03:35.779 # +failover-end master mymaster 127.0.0.1 6666
40717:X 22 Jul 2023 19:03:35.779 # +switch-master mymaster 127.0.0.1 6666 127.0.0.1 6667
40717:X 22 Jul 2023 19:03:35.780 * +slave slave 127.0.0.1:6666 127.0.0.1 6666 @ mymaster 127.0.0.1 6667
40717:X 22 Jul 2023 19:03:38.795 # +sdown slave 127.0.0.1:6666 127.0.0.1 6666 @ mymaster 127.0.0.1 6667


40717:X 22 Jul 2023 19:28:23.578 # -sdown slave 127.0.0.1:6666 127.0.0.1 6666 @ mymaster 127.0.0.1 6667
40717:X 22 Jul 2023 19:28:33.535 * +convert-to-slave slave 127.0.0.1:6666 127.0.0.1 6666 @ mymaster 127.0.0.1 6667
```
1. 127.0.0.1 6666 is master, 127.0.0.1 6667 replicaof it
2. 127.0.0.1 6666 down, from sdown->odown, then vote to select leader to failover select master (send `replicaof no one` to 127.0.0.1 6667 become new master), then promot slave to master role, odown's old master become slave
3. 127.0.0.1 6666 up, conver to slave, replicaof 127.0.0.1 6667

use `info Replication` to check
# reference
* [redis replication](https://redis.io/docs/management/replication/)
* [redis sentinel](https://redis.io/docs/management/sentinel/)
* [redis cluster scaling](https://redis.io/docs/management/scaling/)
* [ledisdb](https://github.com/ledisdb/ledisdb)