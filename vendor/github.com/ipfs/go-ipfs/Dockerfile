FROM alpine:edge
MAINTAINER Lars Gierth <lgierth@ipfs.io>

# There is a copy of this Dockerfile called Dockerfile.fast,
# which is optimized for build time, instead of image size.
#
# Please keep these two Dockerfiles in sync.


# Ports for Swarm TCP, Swarm uTP, API, Gateway, Swarm Websockets
EXPOSE 4001
EXPOSE 4002/udp
EXPOSE 5001
EXPOSE 8080
EXPOSE 8081

# IPFS API to use for fetching gx packages.
# This can be a gateway too, since its read-only API provides all gx needs.
# - e.g. /ip4/172.17.0.1/tcp/8080 if the Docker host
#   has the IPFS gateway listening on the bridge interface
#   provided by Docker's default networking.
# - if empty, the public gateway at ipfs.io is used.
ENV GX_IPFS   ""
# The IPFS fs-repo within the container
ENV IPFS_PATH /data/ipfs
# The default logging level
ENV IPFS_LOGGING ""
# Golang stuff
ENV GOPATH     /go
ENV PATH       /go/bin:$PATH
ENV SRC_PATH   /go/src/github.com/ipfs/go-ipfs

# Expose the fs-repo as a volume.
# start_ipfs initializes an fs-repo if none is mounted
VOLUME $IPFS_PATH

# Get the go-ipfs sourcecode
COPY . $SRC_PATH

RUN apk add --no-cache --virtual .build-deps-ipfs musl-dev gcc go git \
	&& apk add --no-cache tini su-exec bash wget ca-certificates \
	# Setup user
	&& adduser -D -h $IPFS_PATH -u 1000 ipfs \
	# Install gx
	&& go get -u github.com/whyrusleeping/gx \
	&& go get -u github.com/whyrusleeping/gx-go \
	# Point gx to a specific IPFS API
	&& ([ -z "$GX_IPFS" ] || echo $GX_IPFS > $IPFS_PATH/api) \
	# Invoke gx
	&& cd $SRC_PATH \
	&& gx --verbose install --global \
	&& mkdir .git/objects && commit=$(git rev-parse --short HEAD) \
	&& echo "ldflags=-X github.com/ipfs/go-ipfs/repo/config.CurrentCommit=$commit" \
	# Build and install IPFS and entrypoint script
	&& cd $SRC_PATH/cmd/ipfs \
	&& go build -ldflags "-X github.com/ipfs/go-ipfs/repo/config.CurrentCommit=$commit" \
	&& cp ipfs /usr/local/bin/ipfs \
	&& cp $SRC_PATH/bin/container_daemon /usr/local/bin/start_ipfs \
	&& chmod 755 /usr/local/bin/start_ipfs \
	# Remove all build-time dependencies
	&& apk del --purge .build-deps-ipfs && rm -rf $GOPATH && rm -vf $IPFS_PATH/api

# This just makes sure that:
# 1. There's an fs-repo, and initializes one if there isn't.
# 2. The API and Gateway are accessible from outside the container.
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/start_ipfs"]

# Execute the daemon subcommand by default
CMD ["daemon", "--migrate=true"]
