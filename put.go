package main

import (
	"context"
	"io"
	"log"
	"os"
)

type PutCmd struct {
	Common
}

func (cmd *PutCmd) Run(cli *Context) (err error) {
	ctx := context.Background()
	client, err := newClient(cmd.Remote)
	if err != nil {
		return err
	}
	w, err := client.NewWriter(ctx, cmd.Path)
	if err != nil {
		return err
	}
	defer func() {
		if err2 := w.Close(); err2 != nil {
			if err == nil {
				err = err2
			}
		}
	}()

	_, err = io.Copy(w, os.Stdin)
	if err != nil {
		return err
	}
	log.Printf("done putting")
	return nil
}
