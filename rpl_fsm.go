package replica

import (
	"context"
	"io"
	"os"

	storager "github.com/weedge/xdis-storager"
)

// DumpFile dumps data to the file
func (r *Replication) DumpFile(ctx context.Context, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return r.Dump(ctx, f)
}

// Dump dumps data to the Writer.
func (r *Replication) Dump(ctx context.Context, writer io.Writer) error {
	commitID, err := r.LastCommitID()
	if err != nil {
		return err
	}

	err = r.storager.SaveSnapshotWithHead(ctx, &storager.SnapshotHead{CommitID: commitID}, writer)
	return err
}

// LoadDumpFile clears all data and loads dump file to db
func (r *Replication) LoadDumpFile(ctx context.Context, path string) (*storager.SnapshotHead, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return r.LoadDump(ctx, f)
}

// LoadDump clears all data and loads dump file to db
func (r *Replication) LoadDump(ctx context.Context, read io.Reader) (h *storager.SnapshotHead, err error) {
	h, err = r.storager.RecoverFromSnapshotWithHead(ctx, read)
	if err != nil {
		return
	}

	if err = r.UpdateCommitID(h.CommitID); err != nil {
		return
	}

	return
}
