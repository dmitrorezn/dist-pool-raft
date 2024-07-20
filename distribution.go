package main

import (
	"context"
	"fmt"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/pkg/errors"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
)

type ConsensusCfg struct {
	RPCAddr       string        `env:"RPC_ADDR" envDefault:"localhost:10001"`
	RaftAddr      string        `env:"RAFT_ADDR" envDefault:"localhost:8081"`
	BindAddr      string        `env:"BIND_ADDR" envDefault:"localhost:8082"`
	AdvertiseAddr string        `env:"ADVERT_ADDR" envDefault:"localhost:8083"`
	MaxPool       int           `env:"MAX_POOL" envDefault:"5"`
	Timeout       time.Duration `env:"TIMEOUT" envDefault:"10s"`
	LogStoreDir   string        `env:"RAFT_LOG_STORE_DIR" envDefault:"persist"`
	DataStoreDir  string        `env:"RAFT_DATA_STORE_DIR" envDefault:"persist"`
	Bootstrap     bool          `env:"RAFT_BOOTSTRAP" envDefault:"true"`
}

type Consensus struct {
	*raft.Raft

	transport     raft.Transport
	cfg           ConsensusCfg
	stream        *raft.StreamLayer
	raftCfg       raft.Config
	stableStore   raft.StableStore
	logStore      raft.LogStore
	snapshotStore *raft.FileSnapshotStore
	fsm           raft.FSM
}

func NewConsensus(cfg ConsensusCfg, fsm raft.FSM) (*Consensus, error) {
	var consensus = Consensus{
		cfg: cfg,
		fsm: fsm,
	}
	setup := []struct {
		name string
		fn   func() error
	}{
		{"initStableStor", consensus.initStableStor},
		{"initSnapshotStor", consensus.initSnapshotStor},
		{"initLogStor", consensus.initLogStor},
		{"setupTransport", consensus.setupTransport},
		{"initRaft", consensus.initRaft},
		{"bootstrapCluster", consensus.bootstrapCluster},
	}
	for _, s := range setup {
		fmt.Println("start", s.name)
		if err := s.fn(); err != nil {
			return nil, err
		}
		fmt.Println("done", s.name)
	}

	return &consensus, nil
}

func (c *Consensus) setupTransport() error {
	addr, err := net.ResolveTCPAddr("tcp", c.cfg.AdvertiseAddr)
	if err != nil {
		return errors.Wrap(err, "ResolveTCPAddr")
	}
	c.transport, err = raft.NewTCPTransportWithConfig(c.cfg.BindAddr, addr, &raft.NetworkTransportConfig{
		Timeout: c.cfg.Timeout,
	})
	if err != nil {
		return errors.Wrap(err, "NewTCPTransportWithConfig")
	}

	return nil
}

const (
	stableStorePerm = 0755
)

func (c *Consensus) initStableStor() (err error) {
	stableStorDir := filepath.Join(c.cfg.LogStoreDir, "raft")
	if err = os.MkdirAll(stableStorDir, stableStorePerm); err != nil {
		return errors.Wrap(err, "MkdirAll")
	}
	f, err := os.OpenFile(filepath.Join(stableStorDir, "stable"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return errors.Wrap(err, "OpenFile")
	}
	if err = f.Close(); err != nil {
		return errors.Wrap(err, "Close")
	}
	if c.stableStore, err = raftboltdb.NewBoltStore(filepath.Join(stableStorDir, "stable")); err != nil {
		return errors.Wrap(err, "NewBoltStore")
	}
	if c.logStore, err = raftboltdb.NewBoltStore(filepath.Join(stableStorDir, "logs")); err != nil {
		return errors.Wrap(err, "NewBoltStore")
	}

	return
}

func (c *Consensus) initSnapshotStor() (err error) {
	const retain = 1
	c.snapshotStore, err = raft.NewFileSnapshotStore(
		filepath.Join(c.cfg.DataStoreDir, "snapshot"),
		retain,
		os.Stderr,
	)

	return
}

func (c *Consensus) initLogStor() (err error) {
	stableStorDir := filepath.Join(c.cfg.LogStoreDir, "raft")

	if c.logStore, err = raftboltdb.NewBoltStore(filepath.Join(stableStorDir, "logs_store")); err != nil {
		return errors.Wrap(err, "NewBoltStore")
	}

	return
}

const (
	leaderWaitPeriod = time.Second
)

func (c *Consensus) WaitForLeader(ctx context.Context) (string, error) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-timer.C:
		}
		leaderAddr, id := c.Raft.LeaderWithID()
		if leaderAddr != "" && id != "" {
			return string(leaderAddr), nil
		}
		timer.Reset(leaderWaitPeriod)
	}
}

func (c *Consensus) initRaft() (err error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(c.cfg.RaftAddr)

	c.Raft, err = raft.NewRaft(
		config,
		c.fsm,
		c.logStore,
		c.stableStore,
		c.snapshotStore,
		c.transport,
	)
	if err != nil {
		return errors.Wrap(err, "NewRaft <"+c.cfg.RaftAddr+">")
	}

	return err
}

func (c *Consensus) Close() error {
	c.Raft.Shutdown()

	return nil
}

func (c *Consensus) bootstrapCluster() (err error) {
	if !c.cfg.Bootstrap {
		return err
	}
	config := raft.Configuration{
		Servers: []raft.Server{{
			ID:      raft.ServerID(c.cfg.RaftAddr),
			Address: c.transport.LocalAddr(),
		}},
	}

	return c.Raft.BootstrapCluster(config).Error()
}
