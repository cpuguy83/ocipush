package tarpush

import (
	"context"
	"strings"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/platforms"
	"github.com/containerd/containerd/remotes"
	"github.com/containerd/containerd/remotes/docker"
	"github.com/containerd/containerd/remotes/docker/config"
	"github.com/cpuguy83/dockercfg"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

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
