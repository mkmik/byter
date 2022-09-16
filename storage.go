package main

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"

	securejoin "github.com/cyphar/filepath-securejoin"
)

type Storage interface {
	Write(key string, r io.Reader) error
	Read(key string, offset int64) (io.ReadCloser, error)
}

type FileNamer interface {
	PathFor(base, key string) (string, error)
}

type diskStorage struct {
	dir   string
	namer FileNamer
}

func NewDiskStorage(dir string, namer FileNamer) *diskStorage {
	return &diskStorage{dir: dir, namer: namer}
}

func (d *diskStorage) Write(key string, r io.Reader) error {
	path, err := d.namer.PathFor(d.dir, key)
	if err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, r)
	return err
}

func (d *diskStorage) Read(key string, offset int64) (io.ReadCloser, error) {
	path, err := d.namer.PathFor(d.dir, key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	return f, nil
}

type shaFileNamer struct{}

func (shaFileNamer) PathFor(base, key string) (string, error) {
	h := sha256.Sum256([]byte(key))
	return filepath.Join(base, hex.EncodeToString(h[:])), nil
}

type safeFileNamer struct{}

func (safeFileNamer) PathFor(base, key string) (string, error) {
	return securejoin.SecureJoin(base, key)
}
