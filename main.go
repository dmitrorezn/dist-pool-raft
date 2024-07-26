package main

import (
	"context"
	"fmt"
	"github.com/caarlos0/env"
	"github.com/dmitrorezn/dist-tx-pool-raft/internal/domain"
	"github.com/dmitrorezn/dist-tx-pool-raft/internal/pool"
	"github.com/joho/godotenv"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
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

	ctx, cancel := context.WithCancel(context.Background())
	cancel = sync.OnceFunc(cancel)
	defer cancel()
	store, err := NewDB(app.cfg.DiskStoreCfg, transactions)
	if err != nil {
		log.Fatal(err)
	}
	txpool := pool.NewTxPool(ctx, store, app.cfg.BufferCapacity)

	fsm := NewFSM(txpool)

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
		ctx, c := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer c()
		<-ctx.Done()

		cancel()
	}()

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
