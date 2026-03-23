package badger

import (
	"os"
	"testing"

	"github.com/dgraph-io/badger/v4"
	grubbadger "github.com/zoobz-io/grub/badger"
	"github.com/zoobz-io/grub/testing/integration/kv"
)

var tc *kv.TestContext

func TestMain(m *testing.M) {
	tmpDir, err := os.MkdirTemp("", "badger-test-*")
	if err != nil {
		panic("failed to create temp dir: " + err.Error())
	}

	opts := badger.DefaultOptions(tmpDir)
	opts.Logger = nil // silence badger logs in tests

	db, err := badger.Open(opts)
	if err != nil {
		panic("failed to open badger: " + err.Error())
	}

	tc = &kv.TestContext{
		Provider: grubbadger.New(db),
		Cleanup: func() {
			_ = db.Close()
			_ = os.RemoveAll(tmpDir)
		},
	}

	code := m.Run()

	tc.Cleanup()

	os.Exit(code)
}

func TestBadger_CRUD(t *testing.T) {
	kv.RunCRUDTests(t, tc)
}

func TestBadger_Atomic(t *testing.T) {
	kv.RunAtomicTests(t, tc)
}

func TestBadger_TTL(t *testing.T) {
	kv.RunTTLTests(t, tc)
}

func TestBadger_Batch(t *testing.T) {
	kv.RunBatchTests(t, tc)
}

func TestBadger_Hooks(t *testing.T) {
	kv.RunHookTests(t, tc)
}
