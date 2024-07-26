package main

import (
	"bytes"
	"context"
	"encoding/gob"
	errs "errors"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/dmitrorezn/dist-tx-pool-raft/internal/domain"
	"github.com/hashicorp/go-uuid"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
)

type DiskStore struct {
	db         *bolt.DB
	bucketName []byte
}

type DiskStoreCfg struct {
	DiskStoreDir string `env:"DISK_STORE_DIR" envDefault:"persist"`
}

func NewDB(cfg DiskStoreCfg, bucketName string) (ds *DiskStore, err error) {
	ds = &DiskStore{
		bucketName: []byte(bucketName),
	}
	if err = os.MkdirAll(cfg.DiskStoreDir, stableStorePerm); err != nil {
		return nil, err
	}

	if ds.db, err = bolt.Open(
		filepath.Join(cfg.DiskStoreDir, "store"),
		os.ModePerm,
		bolt.DefaultOptions,
	); err != nil {
		return nil, err
	}
	if err = ds.createBucket(bucketName); err != nil {
		return nil, err
	}

	return ds, err
}
func (ds *DiskStore) createBucket(name string) error {
	tx, closer, err := ds.Write()
	if err != nil {
		return err
	}
	defer func() {
		err = closer(err)
	}()
	if _, err = tx.CreateBucket([]byte(name)); err != nil && !errs.Is(err, bolt.ErrBucketExists) {
		return err
	}

	return nil
}

func (ds *DiskStore) Write() (
	tx *bolt.Tx,
	closer func(err error) error,
	err error,
) {
	tx, err = ds.db.Begin(true)

	return tx, func(err error) error {
		if err = errs.Join(tx.Commit(), err); err != nil {
			return errs.Join(tx.Rollback(), err)
		}

		return err
	}, err
}

func (ds *DiskStore) Read() (
	tx *bolt.Tx,
	closer func(err error) error,
	err error,
) {
	tx, err = ds.db.Begin(false)

	return tx, func(err error) error {
		if err = errs.Join(tx.Commit(), err); err != nil {
			return errs.Join(tx.Rollback(), err)
		}

		return err
	}, err
}

const (
	separatorByte = 0x1
)

func (ds *DiskStore) Insert(ctx context.Context, transaction ...*domain.Transaction) (err error) {
	var id string
	if id, err = uuid.GenerateUUID(); err != nil {
		return err
	}
	var buf bytes.Buffer
	for _, tx := range transaction {
		if err = errs.Join(
			buf.WriteByte(separatorByte),
			gob.NewEncoder(&buf).Encode(tx),
		); err != nil {
			return errors.Wrap(err, "Encode")
		}
	}
	if err = ds.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(ds.bucketName).Put([]byte(id), buf.Bytes())
	}); err != nil {
		return errors.Wrap(err, "Update")
	}

	return err
}

//func (ds *DiskStore) GetLast(ctx context.Context) (*Transaction, error) {
//	var transaction *Transaction
//
//	handler := func(tx *bolt.Tx) error {
//		return tx.Bucket(ds.bucketName).ForEach(func(id, v []byte) (err error) {
//			r := bytes.NewBuffer(v)
//			decoder := gob.NewDecoder(r)
//			var separator byte
//			for {
//				var transaction = Transaction{
//					ID: id,
//				}
//				if separator, err = r.ReadByte(); err == io.EOF {
//					return nil
//				}
//				if separator != separatorByte {
//					return fmt.Errorf("wrang leading byte")
//				}
//				if err = decoder.Decode(&transaction); err != nil && err != io.EOF {
//					return errors.Wrap(err, "Decode")
//				}
//				transactions = append(transactions, &transaction)
//			}
//		})
//	}
//
//	return transactions, ds.db.View(handler)
//}

var _ = func() {
	gob.Register(domain.Transaction{})
}

func (ds *DiskStore) listTx(tx *bolt.Tx) ([]*domain.Transaction, error) {
	var transactions []*domain.Transaction

	return transactions, tx.Bucket(ds.bucketName).ForEach(func(id, v []byte) (err error) {
		r := bytes.NewBuffer(v)
		var separator byte

		for {
			var transaction domain.Transaction
			if separator, err = r.ReadByte(); err == io.EOF {
				return nil
			}
			if separator != separatorByte {
				return fmt.Errorf("wrang leading byte")
			}
			if err = gob.NewDecoder(r).Decode(&transaction); err != nil && err != io.EOF {
				return errors.Wrap(err, "Decode")
			}
			transaction.ID = string(id)
			transactions = append(transactions, &transaction)
		}
	})
}
func (ds *DiskStore) List(ctx context.Context) ([]*domain.Transaction, error) {
	var transactions []*domain.Transaction

	return transactions, ds.db.View(func(tx *bolt.Tx) (err error) {
		transactions, err = ds.listTx(tx)

		return err
	})
}

func (ds *DiskStore) Flush(ctx context.Context) error {
	tx, closer, err := ds.Write()
	if err != nil {
		return err
	}
	defer func() {
		err = closer(err)
	}()
	b := tx.Bucket(ds.bucketName)
	cur := b.Cursor()
	for {
		k, v := cur.Next()
		if len(k) == 0 || len(v) == 0 {
			break
		}
		if err = b.Delete(k); err != nil {
			return err
		}
	}

	return err
}
