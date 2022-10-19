## Docker
The method to create multiple architecture images for a single tag pushed to DockerHub is
taken from `https://github.com/cgiraldo/docker-hello-multiarch`.

We created multiple Dockerfiles for creating Docker Image of Dolt for different architectures.
- `Dockerfile` without extension is for 'linux/amd64'
- `Dockerfile.arm64` is for 'linux/arm64'
- No Windows architecture is supported yet.

If new Dockerfile is added to support different OS/architecture, make sure to add appropriate jobs
in `.github/workflows/cd-release.yaml` that updates DOLT_VERSION for the Dockerfile.

The directory `hooks` here include script file that help us create images for different architectures on x86_64 machine.
The Docker documentation on hooks, `https://docs.docker.com/docker-hub/builds/advanced/#custom-build-phase-hooks`

`hooks/post_checkout` installs appropriate version of QEMU tool, `multiarch/qemu-user-static`, which is used
to enable an execution of different multi-architecture containers

`hooks/pre_build` prepares the binary for running shell scripts on specific architecture. It's required for any
OS/architecture except for x86_64.

`hooks/post_push` uses two separate Docker Images for `linux/amd64` and `linux/arm64` to combine them in a single tag
using `docker manifest` command which is 'experimental'. This requires 'experimental' config variable to be set
to 'enabled'.

IMPORTANT:
Currently, we need to set up automated build for each of Dockerfiles for both `latest` and `<release_version>` tags.
For example, we support `linux/amd64` and `linux/arm64`, so we need build for tags,
`latest-amd64`,
`latest-arm64`,
`0.50.4-amd64`,
`0.50.4-arm64`,
if the current release version is '0.50.4'.

-- `COPY docker/qemu-aarch64-static /usr/bin/` is required for building (non x86 architecture image) in x86 host 
before any RUN commands.