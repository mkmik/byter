package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/alecthomas/kong"
	"google.golang.org/grpc"
	"google.golang.org/grpc/binarylog"
)

type Context struct {
	*CLI
}

type CLI struct {
	Get   GetCmd   `cmd:"" help:"Get"`
	Serve ServeCmd `cmd:"" help:"Serve"`
}

func setupBinaryLogging() {
	if _, found := os.LookupEnv("GRPC_BINARY_LOG_FILTER"); !found {
		return
	}

	grpc.EnableTracing = true
	sink, err := binarylog.NewTempFileSink()
	if err != nil {
		log.Fatal(err)
	}
	binarylog.SetSink(sink)
}

func main() {
	setupBinaryLogging()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		binarylog.SetSink(nil)
		os.Exit(0)
	}()

	var cli CLI
	ctx := kong.Parse(&cli)
	err := ctx.Run(&Context{CLI: &cli})
	ctx.FatalIfErrorf(err)
}
