Simple rpmbuild tree and spec to build an RPM of the static Dolt
binary which is installable on all RPM-based Linux distributions.

This RPM does not adhere to typical packaging guidelines for RPM-based
Linux distributions. It does not build the binary artifact from
source, it does not link against distribution-provided runtime
dependencies such as libc, icu4c or zstd, it does not install common
infrastructure pieces like a dedicated user for the server or a
systemd service.

This RPM is mostly useful for installing and uninstalling the
statically linked binaries on RPM-based distributions and allowing
these installations on the host operating systems to be tracked by the
common RPM machinery.

These RPMs are published as part of Dolt releases. Those published RPM
packages are built with a _prefix of /usr/local.

The SOURCES tarballs are symlinks to the tarballs are they get placed in
go/out/ by the go/utils/publishrelease/buildindocker.sh script. Uses this
directory outside the context of buildindocker.sh will require some finessing.
