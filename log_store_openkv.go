package replica

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"

	replicaCfg "github.com/weedge/xdis-replica-storager/config"
	"github.com/weedge/xdis-storager/config"
	"github.com/weedge/xdis-storager/openkv"
)

type OpenkvLogStore struct {
	sync.Mutex
	db *openkv.DB

	cfg *config.OpenkvOptions

	first uint64
	last  uint64

	valBuf bytes.Buffer
	//logKeyBufPool *poolutils.BufferPool
}

func NewOpenkvLogStore(cfg *config.OpenkvOptions) *OpenkvLogStore {
	s := new(OpenkvLogStore)
	if cfg == nil {
		cfg = config.DefaultOpenkvOptions()
	}
	s.cfg = cfg
	s.first = InvalidLogID
	s.last = InvalidLogID
	//s.logKeyBufPool = poolutils.NewBuffPoolWithLen(0, 8)

	return s
}

func (s *OpenkvLogStore) Name() LogStoreName {
	return replicaCfg.RegiterLogStoreOpenkvName
}

func (s *OpenkvLogStore) Open() error {
	var err error
	s.db, err = openkv.Open(s.cfg)
	return err
}

func (s *OpenkvLogStore) FirstID() (uint64, error) {
	s.Lock()
	id, err := s.firstID()
	s.Unlock()

	return id, err
}

func (s *OpenkvLogStore) LastID() (uint64, error) {
	s.Lock()
	id, err := s.lastID()
	s.Unlock()

	return id, err
}

func (s *OpenkvLogStore) firstID() (uint64, error) {
	if s.first != InvalidLogID {
		return s.first, nil
	}

	it := s.db.NewIterator()
	defer it.Close()

	it.SeekToFirst()

	if it.Valid() {
		s.first = binary.BigEndian.Uint64(it.RawKey())
	}

	return s.first, nil
}

func (s *OpenkvLogStore) lastID() (uint64, error) {
	if s.last != InvalidLogID {
		return s.last, nil
	}

	it := s.db.NewIterator()
	defer it.Close()

	it.SeekToLast()

	if it.Valid() {
		s.last = binary.BigEndian.Uint64(it.RawKey())
	}

	return s.last, nil
}

func (s *OpenkvLogStore) GetLog(id uint64, log *Log) error {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, id)
	//buf := s.logKeyBufPool.Get()
	//defer s.logKeyBufPool.Put(buf)
	//binary.Write(buf, binary.BigEndian, id)
	//key := buf.Bytes()

	v, err := s.db.Get(key)
	if err != nil {
		return err
	} else if v == nil {
		return ErrLogNotFound
	} else {
		return log.Decode(bytes.NewBuffer(v))
	}
}

func (s *OpenkvLogStore) StoreLog(log *Log) error {
	s.Lock()
	defer s.Unlock()

	last, err := s.lastID()
	if err != nil {
		return err
	}

	s.valBuf.Reset()
	s.last = InvalidLogID
	if log.ID != last+1 {
		return ErrStoreLogID
	}

	last = log.ID

	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, log.ID)
	//buf := s.logKeyBufPool.Get()
	//defer s.logKeyBufPool.Put(buf)
	//binary.Write(buf, binary.BigEndian, log.ID)
	//key := buf.Bytes()

	if err := log.Encode(&s.valBuf); err != nil {
		return err
	}
	if err = s.db.Put(key, s.valBuf.Bytes()); err != nil {
		return err
	}

	s.last = last
	return nil
}

func (s *OpenkvLogStore) PurgeExpired(n int64) error {
	if n <= 0 {
		return fmt.Errorf("invalid expired time %d", n)
	}

	t := uint32(time.Now().Unix() - int64(n))

	s.Lock()
	defer s.Unlock()

	s.Reset()

	it := s.db.NewIterator()
	it.SeekToFirst()

	w := s.db.NewWriteBatch()
	defer w.Rollback()

	l := new(Log)
	for ; it.Valid(); it.Next() {
		v := it.RawValue()

		if err := l.Unmarshal(v); err != nil {
			return err
		} else if l.CreateTime > t {
			break
		} else {
			w.Delete(it.RawKey())
		}
	}

	if err := w.Commit(); err != nil {
		return err
	}

	return nil
}

func (s *OpenkvLogStore) Sync() error {
	//no other way for sync, so ignore here
	return nil
}

func (s *OpenkvLogStore) Reset() {
	s.first = InvalidLogID
	s.last = InvalidLogID
	s.valBuf.Reset()
}

func (s *OpenkvLogStore) Clear() error {
	s.Lock()
	defer s.Unlock()

	if s.db != nil {
		s.db.Close()
	}

	s.Reset()
	os.RemoveAll(s.cfg.DBPath)

	return s.Open()
}

func (s *OpenkvLogStore) Close() error {
	s.Lock()
	defer s.Unlock()

	if s.db == nil {
		return nil
	}

	err := s.db.Close()
	s.db = nil
	return err
}
