package pool

import (
	"context"
	errs "errors"
	"fmt"
	"github.com/dmitrorezn/channel"
	"github.com/dmitrorezn/dist-tx-pool-raft/internal/domain"
	"github.com/dmitrorezn/dist-tx-pool-raft/pkg/ring"
	"sync"
	"sync/atomic"
	"time"
)

type Logger struct{}

func (Logger) Error(msg string, kv ...any) {
	fmt.Println(append([]any{msg}, kv...))
}

type TxPool struct {
	buffer              *ring.RingQueue[*domain.Transaction]
	store               Store[*domain.Transaction]
	butchSize           int
	bufferFlushInterval time.Duration
	forceFlushBuffer    chan struct{}
	quit                chan struct{}
	closed              atomic.Bool
	logger              Logger
	mut                 sync.Mutex
}

type Store[T any] interface {
	Insert(ctx context.Context, v ...T) error
	List(ctx context.Context) ([]T, error)
	Flush(ctx context.Context) error
	//GetLast(ctx context.Context) (T, error)
}

const (
	defaultButchSize = 100
)

func NewTxPool(ctx context.Context, store Store[*domain.Transaction], capacity int64) *TxPool {
	p := &TxPool{
		buffer:              ring.NewRingQueue[*domain.Transaction](capacity),
		store:               store,
		butchSize:           defaultButchSize,
		bufferFlushInterval: 5 * time.Second,
	}
	go p.run(ctx)

	return p
}

func (p *TxPool) flushBuffer(ctx context.Context) error {
	var txx = make([]*domain.Transaction, 0)
	for p.buffer.Size() > 0 {
		tx, err := p.buffer.Pop()
		if err != nil {
			break
		}
		txx = append(txx, tx)
	}
	if len(txx) == 0 {
		return nil
	}

	return p.store.Insert(ctx, txx...)
}

var ErrClosed = errs.New("pool closed")
var ErrAlreadyClosed = errs.New("pool already closed")

func (p *TxPool) run(ctx context.Context) {
	timer := time.NewTimer(p.bufferFlushInterval)
	defer timer.Stop()

	var err error
	for {
		if p.isClosed() {
			return
		}
		select {
		case <-ctx.Done():
		case <-channel.SelectN(p.forceFlushBuffer, p.quit):
		case <-timer.C:
		}
		if err = errs.Join(p.flushBuffer(ctx), ctx.Err(), err); err != nil {
			p.logger.Error("forceFlushBuffer", "err", err)
			if ctx.Err() != nil {
				return
			}

			continue
		}
	}
}

func (p *TxPool) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return ErrAlreadyClosed
	}
	close(p.quit)

	return nil
}

func (p *TxPool) Sync() {
	select {
	case p.forceFlushBuffer <- struct{}{}:
	default:
	}
}

func (p *TxPool) isClosed() bool {
	return p.closed.Load()
}

func (p *TxPool) Add(ctx context.Context, txx ...*domain.Transaction) error {
	for _, tx := range txx {
		if err := p.buffer.Push(tx); err != nil {
			return err
		}
	}

	return nil
}

//
//func (p *TxPool) GetLast(ctx context.Context) *domain,Transaction {
//	return new(Transaction)
//}

func (p *TxPool) List(ctx context.Context) ([]*domain.Transaction, error) {
	if !p.mut.TryLock() {
		return []*domain.Transaction{}, nil
	}
	defer p.mut.Unlock()

	return p.store.List(ctx)
}

func (p *TxPool) Flash(ctx context.Context) error {
	return p.store.Flush(ctx)
}
