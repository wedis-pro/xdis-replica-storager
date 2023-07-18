package replica

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/weedge/xdis-replica-storager/config"
)

const (
	snapshotTimeFormat = "2006-01-02T15:04:05.999999999"
)

type SnapshotStore struct {
	sync.Mutex
	cfg *config.SnapshotConfig

	names []string

	quit chan struct{}
}

func NewSnapshotStore(cfg *config.SnapshotConfig) *SnapshotStore {
	if len(cfg.Path) == 0 {
		cfg.Path = config.DefaultSnapshotPath + gReplicaId
	}
	if cfg.MaxNum <= 0 {
		cfg.MaxNum = config.DefaultSnapshotMaxNum
	}

	s := &SnapshotStore{}
	s.cfg = cfg
	s.names = make([]string, 0, s.cfg.MaxNum)
	s.quit = make(chan struct{})

	return s
}

func (s *SnapshotStore) Open(ctx context.Context) error {
	if err := os.MkdirAll(s.cfg.Path, 0755); err != nil {
		return err
	}

	if err := s.checkSnapshots(); err != nil {
		return err
	}

	go s.run(ctx)

	return nil
}

func (s *SnapshotStore) Close() {
	close(s.quit)
}

func snapshotName(t time.Time) string {
	return fmt.Sprintf("dmp-%s", t.Format(snapshotTimeFormat))
}

func parseSnapshotName(name string) (time.Time, error) {
	var timeString string
	if _, err := fmt.Sscanf(name, "dmp-%s", &timeString); err != nil {
		println(err.Error())
		return time.Time{}, err
	}
	when, err := time.Parse(snapshotTimeFormat, timeString)
	if err != nil {
		return time.Time{}, err
	}
	return when, nil
}

func (s *SnapshotStore) checkSnapshots() error {
	cfg := s.cfg
	snapshots, err := os.ReadDir(cfg.Path)
	if err != nil {
		klog.Errorf("read %s error: %s", cfg.Path, err.Error())
		return err
	}

	names := []string{}
	for _, info := range snapshots {
		if path.Ext(info.Name()) == ".tmp" {
			klog.Errorf("temp snapshot file name %s, try remove", info.Name())
			os.Remove(path.Join(cfg.Path, info.Name()))
			continue
		}

		if _, err := parseSnapshotName(info.Name()); err != nil {
			klog.Errorf("invalid snapshot file name %s, err: %s", info.Name(), err.Error())
			continue
		}

		names = append(names, info.Name())
	}

	//from old to new
	sort.Strings(names)

	s.names = names

	s.purge(false)

	return nil
}

func (s *SnapshotStore) run(ctx context.Context) {
	t := time.NewTicker(60 * time.Minute)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			s.Lock()
			if err := s.checkSnapshots(); err != nil {
				klog.Errorf("check snapshots error %s", err.Error())
			}
			s.Unlock()
		case <-s.quit:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *SnapshotStore) purge(create bool) {
	var names []string
	maxNum := s.cfg.MaxNum
	num := len(s.names) - maxNum

	if create {
		num++
		if num > len(s.names) {
			num = len(s.names)
		}
	}

	if num > 0 {
		names = append([]string{}, s.names[0:num]...)
		n := copy(s.names, s.names[num:])
		s.names = s.names[0:n]
	}

	for _, name := range names {
		if err := os.Remove(s.snapshotPath(name)); err != nil {
			klog.Errorf("purge snapshot %s error %s", name, err.Error())
		}
	}
}

func (s *SnapshotStore) snapshotPath(name string) string {
	return path.Join(s.cfg.Path, name)
}

type snapshotDumper interface {
	Dump(ctx context.Context, w io.Writer) error
}

type snapshot struct {
	io.ReadCloser
	f *os.File
}

func (st *snapshot) Read(b []byte) (int, error) {
	return st.f.Read(b)
}

func (st *snapshot) Close() error {
	return st.f.Close()
}

func (st *snapshot) Size() int64 {
	s, _ := st.f.Stat()
	return s.Size()
}

// Create full sync snapshot for replica(master) data store --dump--> snapshot file
func (s *SnapshotStore) Create(ctx context.Context, d snapshotDumper) (*snapshot, time.Time, error) {
	s.Lock()
	defer s.Unlock()

	s.purge(true)

	now := time.Now()
	name := snapshotName(now)

	tmpName := name + ".tmp"

	if len(s.names) > 0 {
		lastTime, _ := parseSnapshotName(s.names[len(s.names)-1])
		if now.Nanosecond() <= lastTime.Nanosecond() {
			return nil, time.Time{}, fmt.Errorf("create snapshot file time %s is behind %s ",
				now.Format(snapshotTimeFormat), lastTime.Format(snapshotTimeFormat))
		}
	}

	f, err := os.OpenFile(s.snapshotPath(tmpName), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, time.Time{}, err
	}

	if err := d.Dump(ctx, f); err != nil {
		f.Close()
		os.Remove(s.snapshotPath(tmpName))
		return nil, time.Time{}, err
	}

	f.Close()
	if err := os.Rename(s.snapshotPath(tmpName), s.snapshotPath(name)); err != nil {
		return nil, time.Time{}, err
	}

	if f, err = os.Open(s.snapshotPath(name)); err != nil {
		return nil, time.Time{}, err
	}
	s.names = append(s.names, name)

	return &snapshot{f: f}, now, nil
}

func (s *SnapshotStore) OpenLatest() (*snapshot, time.Time, error) {
	s.Lock()
	defer s.Unlock()

	if len(s.names) == 0 {
		return nil, time.Time{}, nil
	}

	name := s.names[len(s.names)-1]
	t, _ := parseSnapshotName(name)

	f, err := os.Open(s.snapshotPath(name))
	if err != nil {
		return nil, time.Time{}, err
	}

	return &snapshot{f: f}, t, err
}
