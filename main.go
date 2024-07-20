package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"
	"time"

	"encoding/binary"
	"encoding/gob"
	"encoding/json"

	"github.com/caarlos0/env"
	"github.com/hashicorp/raft"
	"github.com/joho/godotenv"

	"github.com/dmitrorezn/dist-tx-pool-raft/internal/domain"
	"github.com/dmitrorezn/dist-tx-pool-raft/internal/pool"
)

type App struct {
	cfg     Config
	service Service
}

type Config struct {
	ConsensusCfg
	DiskStoreCfg
	BufferCapacity int64 `env:"BUFFER_CAPACITY"`
}

func init() {
	_ = godotenv.Load(os.Args[1])
}

const (
	transactions = "transactions"
)

type Pool interface {
	Add(ctx context.Context, txx ...*domain.Transaction) error
	List(ctx context.Context) ([]*domain.Transaction, error)
	Flash(ctx context.Context) error
	Sync()
}

func main() {
	var app = new(App)
	if err := env.Parse(&app.cfg); err != nil {
		log.Fatal(err)
	}
	if err := env.Parse(&app.cfg.ConsensusCfg); err != nil {
		log.Fatal(err)
	}
	if err := env.Parse(&app.cfg.DiskStoreCfg); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Parse")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	fmt.Println("NotifyContext")

	store, err := NewDB(app.cfg.DiskStoreCfg, transactions)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("NewDB")

	txpool := pool.NewTxPool(ctx, store, app.cfg.BufferCapacity)
	fmt.Println("NewTxPool")

	fsm := NewFSM(txpool)
	fmt.Println("NewFSM")

	consensus, err := NewConsensus(app.cfg.ConsensusCfg, fsm)
	if err != nil {
		log.Fatal(err)
	}
	defer consensus.Close()

	svc := app.NewService(
		ctx,
		txpool,
		consensus,
	)
	mux := http.NewServeMux()
	mux.HandleFunc("/add", add(svc))
	mux.HandleFunc("/list", list(svc))

	wg := sync.WaitGroup{}
	defer wg.Wait()

	server := http.Server{
		Handler: mux,
		Addr:    app.cfg.RPCAddr,
	}
	defer server.Shutdown(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err = server.ListenAndServe(); err != nil {
			log.Println("ListenAndServe", err)
		}
	}()
	fmt.Println("started")

	<-ctx.Done()
}

type Service struct {
	consensus  *Consensus
	pool       Pool
	isLeader   bool
	leaderAddr string
}

func (app *App) NewService(ctx context.Context, pool Pool, consensus *Consensus) *Service {
	fmt.Println("NewService")

	var leaderAddr string
	var err error
	if !app.cfg.Bootstrap {
		if leaderAddr, err = consensus.WaitForLeader(ctx); err != nil {
			log.Fatal("NewService -> WaitForLeader", err)
		}
		fmt.Println("leaderAddr", leaderAddr)
	}

	return &Service{
		pool:       pool,
		consensus:  consensus,
		leaderAddr: "leaderAddr",
		isLeader:   string(consensus.transport.LocalAddr()) == leaderAddr,
	}
}

type Cmd[T any] struct {
	Type string
	Data T
}

type cmd int64

const (
	addCmd   cmd = 1
	listCmd  cmd = 2
	flushCmd cmd = 3
)

func (c cmd) size() int {
	return binary.Size(int64(c))
}
func (c cmd) writeTo(w io.Writer) error {
	return binary.Write(w, binary.LittleEndian, int64(c))
}

func (c cmd) read(w io.Reader) error {
	return binary.Read(w, binary.LittleEndian, (*int64)(&c))
}

func (s *Service) Add(ctx context.Context, txx ...*domain.Transaction) error {
	if !s.isLeader {
		d, err := json.Marshal(txx)
		if err != nil {
			return err
		}
		uri := "http://" + path.Join(net.JoinHostPort("localhost", s.leaderAddr), "add")
		resp, err := http.Post(uri, "application/json", bytes.NewReader(d))
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if d, err = io.ReadAll(resp.Body); err != nil {
			return err
		}

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("Add error code: %d data: %s ", resp.StatusCode, string(d))
		}

		return nil
	}

	if err := s.pool.Add(ctx, txx...); err != nil {
		return err
	}

	b := &bytes.Buffer{}
	if err := addCmd.writeTo(b); err != nil {
		return err
	}
	if err := gob.NewEncoder(b).Encode(txx); err != nil {
		return err
	}

	return s.consensus.Apply(b.Bytes(), 5*time.Second).Error()

}

func (s *Service) Flash(ctx context.Context) error {
	//TODO
	return s.pool.Flash(ctx)
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
		return nil
	}
	switch cmd {
	case addCmd:
		var txx []*domain.Transaction
		if err := gob.NewDecoder(r).Decode(&txx); err != nil {
			fmt.Println("Apply  Decode", err)

			return nil
		}
		if err := f.pool.Add(context.Background(), txx...); err != nil {
			fmt.Println("Apply  Add", err)
		}
	case listCmd:
	case flushCmd:

	}

	return nil
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{
		reader: f.pool,
	}, nil
}

// END: fsm_snapshot

// START: fsm_restore
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
