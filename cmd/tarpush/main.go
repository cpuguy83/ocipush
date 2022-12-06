package main

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/log"
	"github.com/cpuguy83/tarpush"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sirupsen/logrus"
)

func main() {
	log.L.Logger.SetLevel(logrus.TraceLevel)
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: tarpush <remote> <path>")
		os.Exit(1)
	}
	if err := do(os.Args[1], os.Args[2]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func do(ref, p string) error {
	f, err := os.Open(p)
	if err != nil {
		return fmt.Errorf("could not open tar file: %w", err)
	}
	defer f.Close()

	idx, data, err := getTarManifestIndex(f)
	if err != nil {
		return fmt.Errorf("could not get manifest digest: %w", err)
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("could not seek to start of tar file: %w", err)
	}

	cp, err := tarpush.NewProvider(f)
	if err != nil {
		return fmt.Errorf("could not create content provider from tar file: %w", err)
	}

	var desc v1.Descriptor
	if idx.MediaType == "" {
		idx.MediaType = v1.MediaTypeImageIndex
		data, err := json.Marshal(idx)
		if err != nil {
			return fmt.Errorf("could not marshal manifest index: %w", err)
		}

		desc = v1.Descriptor{MediaType: idx.MediaType, Size: int64(len(data)), Digest: digest.FromBytes(data)}
		cp = &providerWrap{
			p:    cp,
			rdr:  newBytesReaderAt(data),
			desc: desc,
		}
	} else {
		desc = v1.Descriptor{MediaType: idx.MediaType, Size: int64(len(data)), Digest: digest.FromBytes(data)}
	}

	err = tarpush.Push(context.Background(), cp, ref, desc)
	if err != nil {
		return fmt.Errorf("could not push tar file: %w", err)
	}
	return nil
}

func newBytesReaderAt(b []byte) content.ReaderAt {
	return &contentReaderAt{rdr: bytes.NewReader(b)}
}

type ReaderAtSize interface {
	io.ReaderAt
	Size() int64
}

type contentReaderAt struct {
	rdr ReaderAtSize
}

func (b *contentReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	defer func() {
		log.G(context.Background()).WithField("off", off).WithError(err).WithField("read", n).WithField("total", b.rdr.Size()).Info("ReadAt")
	}()
	return b.rdr.ReadAt(p, off)
}

func (b *contentReaderAt) Size() int64 {
	return b.rdr.Size()
}

func (b *contentReaderAt) Close() error {
	return nil
}

type providerWrap struct {
	p    content.Provider
	rdr  content.ReaderAt
	desc v1.Descriptor
}

func (p *providerWrap) ReaderAt(ctx context.Context, desc v1.Descriptor) (content.ReaderAt, error) {
	if desc.Digest == p.desc.Digest {
		return p.rdr, nil
	}
	return p.p.ReaderAt(ctx, desc)
}

func getTarManifestIndex(rdr io.Reader) (v1.Index, []byte, error) {
	tarRdr := tar.NewReader(rdr)
	for {
		hdr, err := tarRdr.Next()
		if err != nil {
			return v1.Index{}, nil, err
		}

		if hdr.Name != "index.json" {
			continue
		}
		data, err := io.ReadAll(tarRdr)
		if err != nil {
			return v1.Index{}, nil, err
		}

		var idx v1.Index
		if err := json.Unmarshal(data, &idx); err != nil {
			return v1.Index{}, nil, fmt.Errorf("could not unmarshal manifest index: %w", err)
		}
		return idx, data, nil
	}
}
