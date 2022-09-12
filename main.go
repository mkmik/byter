package main

import (
	"github.com/alecthomas/kong"
)

type Context struct {
	*CLI
}

type CLI struct {
	Get   GetCmd   `cmd:"" help:"Get"`
	Serve ServeCmd `cmd:"" help:"Serve"`
}

func main() {
	var cli CLI
	ctx := kong.Parse(&cli)
	err := ctx.Run(&Context{CLI: &cli})
	ctx.FatalIfErrorf(err)
}
