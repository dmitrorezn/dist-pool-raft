package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"log"
	"time"

	"github.com/hashicorp/raft"

	"github.com/dmitrorezn/dist-tx-pool-raft/internal/domain"
)

type Service struct {
	consensus  *Consensus
	pool       Pool
	isLeader   bool
	leaderAddr string
}

func (app *App) NewService(ctx context.Context, pool Pool, consensus *Consensus) *Service {
	fmt.Println("NewService CREATE")

	var leaderAddr string
	var err error
	if leaderAddr, err = consensus.WaitForLeader(ctx); err != nil {
		log.Fatal("NewService -> WaitForLeader", err)
	}
	fmt.Println("leaderAddr", leaderAddr)

	s := &Service{
		pool:       pool,
		consensus:  consensus,
		leaderAddr: leaderAddr,
		isLeader:   string(consensus.transport.LocalAddr()) == leaderAddr,
	}

	go s.run(ctx)

	return s
}
func (s *Service) run(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	var err error
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if s.isLeader {
			if err = s.consensus.VerifyLeader().Error(); err != nil {
				s.isLeader = false
			}

			continue
		}
		if s.leaderAddr, err = s.consensus.WaitForLeader(ctx); err != nil {
			log.Fatal("run -> WaitForLeader", err)
		}
		s.isLeader = string(s.consensus.transport.LocalAddr()) == s.leaderAddr

	}
}

type Cmd[T any] struct {
	Type string
	Data T
}

//go:generate stringer -type cmd
type cmd int64

const (
	undefinedCmd cmd = iota
	addCmd
	listCmd
	flushCmd
)

func (c cmd) size() int {
	return binary.Size(int64(c))
}

func (c cmd) writeTo(w io.Writer) error {
	return binary.Write(w, binary.BigEndian, int64(c))
}

func (c *cmd) read(w io.Reader) error {
	return binary.Read(w, binary.BigEndian, (*int64)(c))
}

func (s *Service) Add(ctx context.Context, txx ...*domain.Transaction) (leader string, err error) {
	fmt.Println("ADD", s.isLeader, txx)
	if s.isLeader {
		if err := s.pool.Add(ctx, txx...); err != nil {
			return "", err
		}

		return "", s.apply(addCmd, txx)
	}

	return s.leaderAddr, nil
}

func (s *Service) apply(cmd cmd, event any) error {
	b := &bytes.Buffer{}
	if err := cmd.writeTo(b); err != nil {
		return err
	}
	fmt.Println("apply I AM LEADER", s.isLeader, cmd.String(), len(b.Bytes()), b.Bytes())
	if err := gob.NewEncoder(b).Encode(event); err != nil {
		return err
	}
	if err := s.consensus.Apply(b.Bytes(), 5*time.Second).Error(); err != nil {
		return errors.Wrap(err, "Apply")
	}

	return nil
}

type Flush struct {
}

func (s *Service) Flash(ctx context.Context) error {
	if s.isLeader {
		if err := s.pool.Flash(ctx); err != nil {
			return err
		}
	}

	return s.apply(flushCmd, Flush{})
}

func (s *Service) List(ctx context.Context) ([]*domain.Transaction, error) {
	return s.pool.List(ctx)
}

func NewFSM(pool Pool) *FSM {
	return &FSM{
		pool: pool,
	}
}

type FSM struct {
	pool Pool
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd cmd

	r := bytes.NewReader(log.Data)

	if err := cmd.read(r); err != nil {
		fmt.Println("Apply  read", err)

		return err
	}
	fmt.Println("Apply CMD", cmd.String())

	switch cmd {
	case addCmd:
		var txx []*domain.Transaction
		if err := gob.NewDecoder(r).Decode(&txx); err != nil {
			fmt.Println("Apply Decode", err)

			return nil
		}
		fmt.Println("Apply addCmd", txx)

		if err := f.pool.Add(context.Background(), txx...); err != nil {
			fmt.Println("Apply Add", err)
		}
	case listCmd:
		panic(listCmd)
	case flushCmd:
		fmt.Println("Apply flushCmd")

		if err := f.pool.Flash(context.Background()); err != nil {
			fmt.Println("Apply Flash", err)
		}
	}

	return nil
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{
		reader: f.pool,
	}, nil
}

func (f *FSM) Restore(r io.ReadCloser) (err error) {
	return nil
	//b := make([]byte, lenWidth)
	//var buf bytes.Buffer
	//for i := 0; ; i++ {
	//	if _, err := io.ReadFull(r, b); err == io.EOF {
	//		break
	//	} else if err != nil {
	//		return err
	//	}
	//	size := int64(enc.Uint64(b))
	//	if _, err = io.CopyN(&buf, r, size); err != nil {
	//		return err
	//	}
	//	record := &api.Record{}
	//	var tx Transa
	//	if err = gob.NewDecoder(buf), record); err != nil {
	//		return err
	//	}
	//	if i == 0 {
	//		f.log.Config.Segment.InitialOffset = record.Offset
	//		if err := f.log.Reset(); err != nil {
	//			return err
	//		}
	//	}
	//	if _, err = f.log.Append(record); err != nil {
	//		return err
	//	}
	//	buf.Reset()
	//}
	//
	return nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader Pool
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	s.reader.Sync()

	return sink.Close()
}

func (s *snapshot) Release() {}
