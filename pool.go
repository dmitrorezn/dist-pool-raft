package main

import (
	"context"
	errs "errors"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type Transaction struct {
	ID        []byte
	Index     int
	Timestamp int64
	Encoding  int
	Data      []byte
	Action    string
}

type Logger struct{}

func (Logger) Error(msg string, kv ...any) {
	fmt.Println(append([]any{msg}, kv...))
}

type Buffer[T any] interface {
	Push(T)
	PushN(...T)
	Pop() (T, error)
	Size() int
	Cap() int
	PopN(int) ([]T, error)
}

type TxPool struct {
	buffer              Buffer[*Transaction]
	store               Store[*Transaction]
	butchSize           int
	bufferFlushInterval time.Duration
	forceFlushBuffer    chan struct{}
	quit                chan struct{}
	logger              Logger
	mut                 sync.Mutex
}

type Store[T any] interface {
	Insert(ctx context.Context, v ...T) (string, error)
	List(ctx context.Context) ([]T, error)
	Flush(ctx context.Context) error
}

const (
	defaultButchSize = 100
)

func NewTxPool(ctx context.Context, buffer Buffer[*Transaction], store Store[*Transaction]) *TxPool {
	p := &TxPool{
		buffer:              buffer,
		store:               store,
		butchSize:           defaultButchSize,
		bufferFlushInterval: 5 * time.Second,
	}
	go p.run(ctx)
	return p
}

func (p *TxPool) flushBuffer(ctx context.Context) error {
	var txx = make([]*Transaction, 0)
	for {
		tx, err := p.buffer.Pop()
		if err != nil {
			break
		}
		txx = append(txx, tx)
	}
	if len(txx) == 0 {
		return nil
	}
	_, err := p.store.Insert(ctx, txx...)

	return err
}

var ErrClosed = errors.New("pool closed")

func (p *TxPool) run(ctx context.Context) {
	timer := time.NewTimer(p.bufferFlushInterval)
	defer timer.Stop()

	var err error
	for {
		select {
		case <-p.forceFlushBuffer:
		case <-timer.C:
		case <-ctx.Done():
			err = ctx.Err()
		case <-p.quit:
			err = ErrClosed
		}
		if err = errs.Join(p.flushBuffer(ctx), err); err != nil {
			p.logger.Error("forceFlushBuffer", "err", err)

			continue
		}
	}
}

func (p *TxPool) Close() {
	close(p.quit)
}

func (p *TxPool) Add(ctx context.Context, tx ...*Transaction) error {
	p.buffer.PushN(tx...)

	return nil
}

func (p *TxPool) GetLast(ctx context.Context) *Transaction {
	return new(Transaction)
}

func (p *TxPool) List(ctx context.Context) ([]*Transaction, error) {
	if !p.mut.TryLock() {
		return []*Transaction{}, nil
	}
	defer p.mut.Unlock()

	return p.store.List(ctx)
}

func (p *TxPool) Flash(ctx context.Context) error {
	return p.store.Flush(ctx)
}
