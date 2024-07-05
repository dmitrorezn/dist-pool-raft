package main

import (
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
)

var defaultTransportCfg = &ConsensusCfg{
	BindAddr:      ":90",
	AdvertiseAddr: ":90",
	MaxPool:       5,
	Timeout:       10 * time.Second,
}

func (c *Consensus) setupTransport(sr raft.StreamLayer, cfg *ConsensusCfg) raft.Transport {
	return raft.NewNetworkTransport(sr, cfg.MaxPool, cfg.Timeout, os.Stdout)
}

type ConsensusCfg struct {
	RaftAddr      string        `env:"RAFT_ADDR"`
	BindAddr      string        `env:"BIND_ADDR"`
	AdvertiseAddr string        `env:"ADVERT_ADDR"`
	MaxPool       int           `env:"MAX_POOL"`
	Timeout       time.Duration `env:"TIMEOUT"`
	LogStoreDir   string        `env:"RAFT_LOG_STORE_DIR"`
}

type Consensus struct {
	raft.Transport
	cfg         ConsensusCfg
	stream      *raft.StreamLayer
	raftCfg     raft.Config
	raft        *raft.Raft
	stableStore raft.StableStore
}

func NewConsensus(cfg ConsensusCfg, service *Service) (*Consensus, error) {
	var consensus Consensus

	setup := []func() error{
		consensus.initStableStor,
		consensus.initRaft,
	}
	for _, s := range setup {
		if err := s(); err != nil {
			return nil, err
		}
	}

	return &consensus, nil
}

const (
	stableStorePerm = 0755
)

func (c *Consensus) initStableStor() (err error) {
	stableStorDir := filepath.Join(c.cfg.LogStoreDir, "raft", "stable")
	if err := os.MkdirAll(stableStorDir, stableStorePerm); err != nil {
		return err
	}
	c.stableStore, err = raftboltdb.NewBoltStore(stableStorDir)

	return
}

func (c *Consensus) initRaft() (err error) {
	//c.raft, err = raft.NewRaft(
	//	&raft.Config{},
	//)

	return
}
