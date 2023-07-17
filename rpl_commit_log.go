package replica

import (
	"encoding/binary"
	"io"
	"os"
	"path"

	"github.com/weedge/xdis-replica-storager/config"
)

// CommitLog save current commited logId file
type CommitLog struct {
	id   uint64
	file *os.File
}

func (m *CommitLog) Close() error {
	err := m.file.Close()
	return err
}

// InitCommitLog  open commit.log file to init latest commited logId
func (m *CommitLog) InitCommitLog(cfg *config.ReplicationConfig) (err error) {
	if m == nil {
		return
	}

	if m.file, err = os.OpenFile(path.Join(cfg.Path, "commit.log"), os.O_RDWR|os.O_CREATE, 0644); err != nil {
		return
	}

	if s, _ := m.file.Stat(); s.Size() == 0 {
		m.id = 0
	} else if err = binary.Read(m.file, binary.BigEndian, &m.id); err != nil {
		return
	}

	return
}

// UpdateCommitID update current commited logId to commit.log file
func (m *CommitLog) UpdateCommitID(id uint64, force bool) error {
	if force {
		if _, err := m.file.Seek(0, io.SeekStart); err != nil {
			return err
		}

		if err := binary.Write(m.file, binary.BigEndian, id); err != nil {
			return err
		}
	}

	m.id = id
	return nil
}
