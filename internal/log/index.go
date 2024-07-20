package log

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"io"
)

type IndexStore struct {
	*store
	pos int
}

func New(s *store) *IndexStore {
	return &IndexStore{
		store: s,
	}
}

func (s *IndexStore) FirstIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var i uint64

	defer fmt.Println("FirstIndex", i)
	return i, binary.Read(s.store.File, enc, &i)
}

func (s *IndexStore) LastIndex() (uint64, error) {
	defer fmt.Println("LastIndex", s.store.size)

	return uint64(s.pos), nil
}

func (s *IndexStore) GetLog(index uint64, log *raft.Log) error {
	if s.store.size == 0 {
		return raft.ErrLogNotFound
	}
	var buf = make([]byte, binary.Size(uint64(0)))
	fmt.Println("GetLog size", s.store.size, index)
	n, err := s.store.ReadAt(buf, int64(index))
	if err != nil {
		return errors.Wrap(err, "ReadAt")
	}
	size := enc.Uint64(buf[:n])
	section := io.NewSectionReader(s.store, int64(index+uint64(n)), int64(size))

	if err = gob.NewDecoder(section).Decode(log); err != nil {
		return errors.Wrap(err, "Decode")
	}

	return err
}

func (s *IndexStore) StoreLog(log *raft.Log) error {
	b := &bytes.Buffer{}
	gob.NewEncoder(b).Encode(log)
	var err error
	s.pos, err = s.store.Write(b.Bytes())
	return err
}

func (s *IndexStore) StoreLogs(logs []*raft.Log) error {
	for _, log := range logs {
		if err := gob.NewEncoder(s.store).Encode(log); err != nil {
			return err
		}
	}

	return nil
}

func (s *store) DeleteRange(_, max uint64) error {
	return s.Truncate(int64(max))
}
