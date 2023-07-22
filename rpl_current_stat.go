package replica

type Stat struct {
	// FirstID from log store
	FirstID uint64
	// LastID from log store
	LastID uint64
	// CommitId from current commit log, init load to CommitLog.id
	CommitID uint64
}

func (r *Replication) Stat() (s *Stat, err error) {
	r.Lock()
	defer r.Unlock()

	s = &Stat{}
	if s.FirstID, err = r.logStore.FirstID(); err != nil {
		return nil, err
	}

	if s.LastID, err = r.logStore.LastID(); err != nil {
		return nil, err
	}

	s.CommitID = r.commitLog.id
	return s, nil
}

func (r *Replication) StoreLog(log *Log) error {
	r.Lock()
	err := r.logStore.StoreLog(log)
	r.Unlock()

	return err
}

func (r *Replication) FirstLogID() (uint64, error) {
	r.Lock()
	id, err := r.logStore.FirstID()
	r.Unlock()

	return id, err
}

func (r *Replication) LastLogID() (uint64, error) {
	r.Lock()
	id, err := r.logStore.LastID()
	r.Unlock()
	return id, err
}

func (r *Replication) LastCommitID() (uint64, error) {
	r.Lock()
	id := r.commitLog.id
	r.Unlock()
	return id, nil
}
