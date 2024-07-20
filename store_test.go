package main

import (
	"context"

	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dmitrorezn/dist-tx-pool-raft/internal/domain"
)

func TestDiskStore_Insert(t *testing.T) {
	_ = os.Mkdir("test", 0750)

	store, err := NewDB(DiskStoreCfg{
		DiskStoreDir: "test",
	}, "test-tx")
	assert.NoError(t, err)

	ctx := context.Background()
	t.Run("insert", func(t *testing.T) {
		tx := &domain.Transaction{
			Index:     1,
			ID:        "test",
			Timestamp: time.Now().UnixNano(),
			Data:      []byte("test"),
			Action:    "tx",
			Encoding:  1,
		}
		err := store.Insert(ctx, tx)
		assert.NoError(t, err)

		txx, err := store.List(ctx)
		assert.NoError(t, err)
		if assert.Len(t, txx, 1) {
			assert.Equal(t, tx, txx[0])
		}
	})

	assert.NoError(t, os.Remove("test/store"))
}
