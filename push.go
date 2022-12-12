package ocipush

import (
	"context"
	"io/fs"
	"strings"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/platforms"
	"github.com/containerd/containerd/remotes"
	"github.com/containerd/containerd/remotes/docker"
	"github.com/containerd/containerd/remotes/docker/config"
	"github.com/cpuguy83/dockercfg"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

// Push is a helper function to push content from the providced content.Provider.
// This is just a wrapper around containerd's `remotes.PushContent` which sets up a remotes.Resolver using the provdied ref.
// The generated resolver will use whatever autth is set in the Docker CLI's config.
func Push(ctx context.Context, provider content.Provider, ref string, desc v1.Descriptor) error {
	r := docker.NewResolver(docker.ResolverOptions{
		Hosts: config.ConfigureHosts(ctx, config.HostOptions{
			Credentials: dockercfg.GetRegistryCredentials,
		}),
	})

	if !strings.Contains(ref, "@") {
		ref = ref + "@" + desc.Digest.String()
	}
	p, err := r.Pusher(ctx, ref)
	if err != nil {
		return err
	}
	return remotes.PushContent(ctx, p, desc, provider, nil, platforms.All, nil)
}

// NewProvider creates a containerd content.Provider from the given fs.FS.
// The FS is expected to use the oci-layout.
func NewProvider(fs fs.FS) (content.Provider, error) {
	return &FsProvider{
		Fs: fs,
	}, nil
}

// FsProvider is a containerd content.Provider that uses an fs.FS to implement
// the content.Provider interface.
// The FS must be an OCI layout.
type FsProvider struct {
	Fs fs.FS
}

func (p *FsProvider) ReaderAt(ctx context.Context, desc v1.Descriptor) (content.ReaderAt, error) {
	f, err := p.Fs.Open("blobs/" + desc.Digest.Algorithm().String() + "/" + desc.Digest.Encoded())
	if err != nil {
		return nil, err
	}
	return f.(content.ReaderAt), nil
}
