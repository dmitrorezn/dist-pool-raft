package main

import (
	"context"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestDiskStore_Insert(t *testing.T) {
	_ = os.Mkdir("test", 0750)

	store, err := NewDB(DiskStoreCfg{
		DiskStoreDir: "test",
	}, "test-tx")
	assert.NoError(t, err)

	ctx := context.Background()
	t.Run("insert", func(t *testing.T) {
		tx := &Transaction{
			Index:     1,
			Timestamp: time.Now().UnixNano(),
			Data:      []byte("test"),
			Action:    "tx",
			Encoding:  1,
		}
		id, err := store.Insert(ctx, tx)
		assert.NoError(t, err)
		tx.ID = []byte(id)

		txx, err := store.List(ctx)
		assert.NoError(t, err)
		if assert.Len(t, txx, 1) {
			assert.Equal(t, tx, txx[0])
		}
	})

	assert.NoError(t, os.Remove("test/store"))
}
