package lsm

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"strconv"
	"testing"

	"github.com/golang-cz/devslog"
)

func TestLSMTree(t *testing.T) {
	logOpts := &devslog.Options{HandlerOptions: &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: true,
	}}
	slog.SetDefault(slog.New(devslog.NewHandler(os.Stdout, logOpts)))

	opts := DefaultOptions
	opts.MemtableThreshold = 1 << 5
	tree, err := Recover(context.Background(), "./db", opts)
	if err != nil {
		t.Errorf("recovering: %s", err.Error())
	}

	for i := 0; i < 100; i++ {
		v := bytes.Repeat([]byte(strconv.Itoa(i)), 1024)
		if err := tree.Put([]byte(strconv.Itoa(i)), v); err != nil {
			t.Errorf("putting: %s", err.Error())
		}
	}

	value, err := tree.Get([]byte(strconv.Itoa(42)))
	if err != nil {
		t.Error("getting: %w", err)
	}

	t.Logf("value: %s", value)
}
