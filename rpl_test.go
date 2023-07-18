package replica

import (
	"testing"

	"github.com/weedge/xdis-storager/driver"
)

func TestImpICommitter(t *testing.T) {
	var i interface{} = &Replication{}
	if _, ok := i.(driver.ICommitter); !ok {
		t.Fatalf("does not implement driver.ICommitter")
	}
}
