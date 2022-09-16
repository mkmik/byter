package main

import (
	context "context"
	"errors"
	"io"
	"log"
	"net"

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
	storage    Storage
	write      bool
	bufferSize uint64
}

// QueryWriteStatus implements bytestream.ByteStreamServer
func (*server) QueryWriteStatus(context.Context, *pb.QueryWriteStatusRequest) (*pb.QueryWriteStatusResponse, error) {
	panic("unimplemented")
}

// Read implements bytestream.ByteStreamServer
func (srv *server) Read(req *pb.ReadRequest, res pb.ByteStream_ReadServer) error {
	f, err := srv.storage.Read(req.ResourceName, req.ReadOffset)
	if err != nil {
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

	var errCh chan error
	pr, pw := io.Pipe()

	n := int64(0)
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			if errCh == nil {
				return status.Errorf(codes.InvalidArgument, "first WriteRequest must contain ResourceName")
			}
			if err := <-errCh; err != nil {
				return err
			}
			return stream.SendAndClose(&pb.WriteResponse{CommittedSize: n})
		}

		if errCh == nil {
			if chunk.WriteOffset != 0 {
				return status.Errorf(codes.Unimplemented, "Apending to files is not implemented (write_offset = %d)", chunk.WriteOffset)
			}

			errCh = make(chan error, 1)
			go func() {
				err := srv.storage.Write(chunk.ResourceName, pr)
				pr.CloseWithError(err)
				errCh <- err
			}()
		}

		written, err := pw.Write(chunk.Data)
		if err != nil {
			return err
		}
		n += int64(written)

		if chunk.FinishWrite {
			pw.Close()
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
		storage:    NewDiskStorage(cmd.Dir, shaFileNamer{}),
		write:      cmd.Write,
		bufferSize: cmd.BufferSize,
	})
	log.Printf("Serving gRPC at %q", cmd.Listen)
	return srv.Serve(listener)
}
