package tarpush

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/containerd/containerd/content"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

func NewProvider(r io.ReaderAt) (content.Provider, error) {
	return &tarContentProvider{
		rdr: io.NewSectionReader(r, 0, 1<<63-1),
	}, nil
}

type tarContentProvider struct {
	tar *tar.Reader
	rdr *io.SectionReader
	idx map[digest.Digest]content.ReaderAt
}

func (t *tarContentProvider) addIdx(dgst digest.Digest, ra content.ReaderAt) {
	if t.idx == nil {
		t.idx = make(map[digest.Digest]content.ReaderAt)
	}
	t.idx[dgst] = ra
}

func (t *tarContentProvider) ReaderAt(ctx context.Context, desc v1.Descriptor) (content.ReaderAt, error) {
	if ra := t.idx[desc.Digest]; ra != nil {
		return ra, nil
	}

	hasher, dgst, found := strings.Cut(desc.Digest.String(), ":")
	if !found {
		return nil, fmt.Errorf("invalid digest %s", desc.Digest)
	}

	if t.tar == nil {
		t.tar = tar.NewReader(t.rdr)
	}

	for {
		hdr, err := t.tar.Next()
		if err != nil {
			return nil, err
		}

		if hdr.Name == "index.json" {
			buf := make([]byte, hdr.Size)
			if _, err := io.ReadFull(t.tar, buf); err != nil {
				return nil, err
			}
			dgst := digest.FromBytes(buf)
			rdr := &contentReaderAt{ra: bytes.NewReader(buf)}
			t.addIdx(dgst, rdr)
			if dgst == desc.Digest {
				return rdr, nil
			}
			continue
		}

		if hdr.Name == "blobs/" || !strings.HasPrefix(hdr.Name, "blobs/") {
			// not a blob we care about
			continue
		}

		split := strings.Split(hdr.Name, "/")
		if len(split) != 3 {
			// probably a digest name (e.g. "sha256")
			continue
		}

		pos, err := t.rdr.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, fmt.Errorf("error getting reader offset: %w", err)
		}
		rdr := &contentReaderAt{
			ra: io.NewSectionReader(t.rdr, pos, hdr.Size),
		}
		t.addIdx(digest.Digest(split[1]+":"+split[2]), rdr)

		if split[1] == hasher && split[2] == dgst {
			return rdr, nil
		}
	}
}

type ReaderAtSize interface {
	io.ReaderAt
	Size() int64
}

type contentReaderAt struct {
	ra ReaderAtSize
}

func (r *contentReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	return r.ra.ReadAt(p, off)
}

func (r *contentReaderAt) Close() error {
	return nil
}

func (r *contentReaderAt) Size() int64 {
	return r.ra.Size()
}
