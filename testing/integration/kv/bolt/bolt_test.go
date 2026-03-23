package bolt

import (
	"os"
	"path/filepath"
	"testing"

	grubbolt "github.com/zoobz-io/grub/bolt"
	"github.com/zoobz-io/grub/testing/integration/kv"
	"go.etcd.io/bbolt"
)

var tc *kv.TestContext

func TestMain(m *testing.M) {
	tmpDir, err := os.MkdirTemp("", "bolt-test-*")
	if err != nil {
		panic("failed to create temp dir: " + err.Error())
	}

	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		panic("failed to open bolt: " + err.Error())
	}

	tc = &kv.TestContext{
		Provider: grubbolt.New(db, "test-bucket"),
		Cleanup: func() {
			_ = db.Close()
			_ = os.RemoveAll(tmpDir)
		},
	}

	code := m.Run()

	tc.Cleanup()

	os.Exit(code)
}

func TestBolt_CRUD(t *testing.T) {
	kv.RunCRUDTests(t, tc)
}

func TestBolt_Atomic(t *testing.T) {
	kv.RunAtomicTests(t, tc)
}

// Note: Bolt does not support TTL, so we skip TTL tests.

func TestBolt_Batch(t *testing.T) {
	kv.RunBatchTests(t, tc)
}

func TestBolt_Hooks(t *testing.T) {
	kv.RunHookTests(t, tc)
}
