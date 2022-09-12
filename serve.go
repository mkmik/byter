package main

import (
	context "context"
	"errors"
	"io"
	"log"
	"net"
	"os"

	securejoin "github.com/cyphar/filepath-securejoin"
	pb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type ServeCmd struct {
	Listen     string `name:"listen" required:"" help:"listen address"`
	Dir        string `name:"dir" required:"" help:"Base directory"`
	BufferSize uint64 `name:"buffer-size" optional:"" default:"1048576" help:"Buffer size; max gprc chunk size."`
}

type server struct {
	pb.UnimplementedByteStreamServer
	dir        string
	bufferSize uint64
}

// QueryWriteStatus implements bytestream.ByteStreamServer
func (*server) QueryWriteStatus(context.Context, *pb.QueryWriteStatusRequest) (*pb.QueryWriteStatusResponse, error) {
	panic("unimplemented")
}

// Read implements bytestream.ByteStreamServer
func (srv *server) Read(req *pb.ReadRequest, res pb.ByteStream_ReadServer) error {
	secpath, err := securejoin.SecureJoin(srv.dir, req.ResourceName)
	if err != nil {
		return err
	}
	f, err := os.Open(secpath)
	if err != nil {
		return err
	}
	if _, err := f.Seek(int64(req.ReadOffset), io.SeekStart); err != nil {
		return err
	}
	b := make([]byte, srv.bufferSize)
	for {
		n, err := f.Read(b)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
		}
		if err := res.Send(&pb.ReadResponse{Data: b[:n]}); err != nil {
			return err
		}
	}
	return nil
}

// Write implements bytestream.ByteStreamServer
func (*server) Write(pb.ByteStream_WriteServer) error {
	panic("unimplemented")
}

func (cmd *ServeCmd) Run(cli *Context) error {
	listener, err := net.Listen("tcp", cmd.Listen)
	if err != nil {
		return err
	}
	srv := grpc.NewServer()
	reflection.Register(srv)
	pb.RegisterByteStreamServer(srv, &server{
		dir:        cmd.Dir,
		bufferSize: cmd.BufferSize,
	})
	log.Printf("Serving gRPC at %q", cmd.Listen)
	return srv.Serve(listener)
}
