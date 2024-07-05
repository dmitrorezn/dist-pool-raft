package main

import (
	"context"
	"fmt"
	"github.com/caarlos0/env"
	"os/signal"
	"syscall"
)

type App struct {
	cfg     Config
	service Service
}
type Service struct {
	Set func()
	Get func()
}

type RaftConfig struct {
}

type Config struct {
	ConsensusCfg
}

func (s *Service) shutdown() {
	fmt.Println("shutdown")
}

func main() {
	var svc = new(App)
	if err := env.Parse(&svc.cfg); err != nil {
		panic(err)
	}
	//if err := svc.setupTransport(); err != nil {
	//	panic(err)
	//}
	//fmt.Println("LocalAddr", svc.Transport.LocalAddr())

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	<-ctx.Done()
}
