package main

import (
	"context"
	"io"
	"os"

	"google.golang.org/api/transport/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Common struct {
	Remote string `arg:"" name:"remote" help:"remote endpoint address"`
	Path   string `arg:"" name:"path" help:"file path"`
}

type GetCmd struct {
	Common
}

func newClient(ctx context.Context, remote string) (*bytestream.Client, error) {
	conn, err := grpc.DialContext(ctx, remote, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return bytestream.NewClient(conn), nil
}

func (cmd *GetCmd) Run(cli *Context) error {
	ctx := context.Background()
	client, err := newClient(ctx, cmd.Remote)
	if err != nil {
		return err
	}
	r, err := client.NewReader(ctx, cmd.Path)
	if err != nil {
		return err
	}
	_, err = io.Copy(os.Stdout, r)
	if err != nil {
		return err
	}
	return nil
}
