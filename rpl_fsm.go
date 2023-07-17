package replica

import (
	"io"
	"os"

	storager "github.com/weedge/xdis-storager"
)

// DumpFile dumps data to the file
func (r *Replication) DumpFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return r.Dump(f)
}

// Dump dumps data to the Writer.
func (r *Replication) Dump(writer io.Writer) error {
	commitID, err := r.LastCommitID()
	if err != nil {
		return err
	}

	err = r.storager.SaveSnapshotWithHead(&storager.SnapshotHead{CommitID: commitID}, writer)
	return err
}

// LoadDumpFile clears all data and loads dump file to db
func (r *Replication) LoadDumpFile(path string) (*storager.SnapshotHead, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return r.LoadDump(f)
}

// LoadDump clears all data and loads dump file to db
func (r *Replication) LoadDump(read io.Reader) (h *storager.SnapshotHead, err error) {
	h, err = r.storager.RecoverFromSnapshotWithHead(read)
	if err != nil {
		return
	}

	if err = r.UpdateCommitID(h.CommitID); err != nil {
		return
	}

	return
}
