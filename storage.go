package main

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
)

type Storage interface {
	Write(key string, r io.Reader) error
	Read(key string, offset int64) (io.ReadCloser, error)
}

type diskStorage struct {
	dir string
}

func NewDiskStorage(dir string) *diskStorage {
	return &diskStorage{dir: dir}
}

func (d *diskStorage) Write(key string, r io.Reader) error {
	f, err := os.Create(d.pathForKey(key))
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, r)
	return err
}

func (d *diskStorage) Read(key string, offset int64) (io.ReadCloser, error) {
	f, err := os.Open(d.pathForKey(key))
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	return f, nil
}

func (d *diskStorage) pathForKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return filepath.Join(d.dir, hex.EncodeToString(h[:]))
}
