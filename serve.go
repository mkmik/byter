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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

type ServeCmd struct {
	Listen     string `name:"listen" required:"" help:"listen address"`
	Dir        string `name:"dir" required:"" help:"Base directory"`
	Write      bool   `name:"write" optional:"" help:"If true, writes are allowed"`
	BufferSize uint64 `name:"buffer-size" optional:"" default:"1048576" help:"Buffer size; max gprc chunk size."`
}

type server struct {
	pb.UnimplementedByteStreamServer
	dir        string
	write      bool
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
func (srv *server) Write(stream pb.ByteStream_WriteServer) error {
	if !srv.write {
		return status.Errorf(codes.Unimplemented, "write support administratively disabled")
	}

	var f *os.File

	n := int64(0)
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			if f == nil {
				return status.Errorf(codes.InvalidArgument, "first WriteRequest must contain ResourceName")
			}
			return stream.SendAndClose(&pb.WriteResponse{CommittedSize: n})
		}

		if f == nil {
			if chunk.WriteOffset != 0 {
				return status.Errorf(codes.Unimplemented, "Apending to files is not implemented (write_offset = %d)", chunk.WriteOffset)
			}

			secpath, err := securejoin.SecureJoin(srv.dir, chunk.ResourceName)
			if err != nil {
				return err
			}
			f, err = os.Create(secpath)
			if err != nil {
				return err
			}
			defer f.Close()
		}

		written, err := f.Write(chunk.Data)
		if err != nil {
			return err
		}
		n += int64(written)

		if chunk.FinishWrite {
			f.Close()
		}
	}
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
		write:      cmd.Write,
		bufferSize: cmd.BufferSize,
	})
	log.Printf("Serving gRPC at %q", cmd.Listen)
	return srv.Serve(listener)
}
