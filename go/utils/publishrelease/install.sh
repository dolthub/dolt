#!/bin/bash

# This script installs dolt on your Linux or macOS computer.
# It should be run as root, and can be run directly from a GitHub
# release, for example as:
#
#   curl https://github.com/dolthub/dolt/releases/download/v__DOLT_VERSION__/install.sh | sudo bash
#
# All downloads occur over HTTPS from the Github releases page.

if test -z "$BASH_VERSION"; then
  echo "Please run this script using bash, not sh or any other shell." >&2
  exit 1
fi

_() {

set -euo pipefail

DOLT_VERSION='__DOLT_VERSION__'
RELEASES_BASE_URL="https://github.com/dolthub/dolt/releases/download/v$DOLT_VERSION"
INSTALL_URL="$RELEASES_BASE_URL/install.sh"

CURL_USER_AGENT="${CURL_USER_AGENT:-dolt-installer}"

OS=
ARCH=
WORK_DIR=

PLATFORM_TUPLE=

error() {
  if [ "$#" != 0 ]; then
    printf '\e[0;31m%s\e[0m\n' "$*" >&2
  fi
}

fail() {
  local error_code="$1"
  shift
  echo '*** INSTALLATION FAILED ***' >&2
  echo '' >&2
  error "$@"
  echo '' >&2
  exit 1
}

assert_linux_or_macos() {
  OS="$(uname)"
  ARCH="$(uname -m)"
  if [ "$OS" != 'Linux' ] && [ "$OS" != 'Darwin' ]; then
    fail 'E_UNSUPPORTED_OS' 'dolt install.sh only supports macOS and Linux.'
  fi

  # Translate aarch64 to arm64, since that's what GOARCH calls it
  if [ "$ARCH" == 'aarch64' ]; then
    ARCH='arm64'
  fi

  if [ "$ARCH-$OS" != 'x86_64-Linux' ] && [ "$ARCH-$OS" != 'x86_64-Darwin' ] && [ "$ARCH-$OS" != 'arm64-Linux' ] && [ "$ARCH-$OS" != 'arm64-Darwin' ]; then
    fail 'E_UNSUPPOSED_ARCH' 'dolt install.sh only supports installing dolt on Linux-x86_64, Darwin-x86_64, Linux-aarch64, or Darwin-arm64.'
  fi

  if [ "$OS" == 'Linux' ]; then
    PLATFORM_TUPLE=linux
  else
    PLATFORM_TUPLE=darwin
  fi

  if [ "$ARCH" == 'x86_64' ]; then
    PLATFORM_TUPLE="$PLATFORM_TUPLE-amd64"
  else
    PLATFORM_TUPLE="$PLATFORM_TUPLE-arm64"
  fi
}

assert_dependencies() {
  type -p curl > /dev/null || fail 'E_CURL_MISSING' 'Please install curl(1).'
  type -p tar > /dev/null || fail 'E_TAR_MISSING' 'Please install tar(1).'
  type -p uname > /dev/null || fail 'E_UNAME_MISSING' 'Please install uname(1).'
  type -p install > /dev/null || fail 'E_INSTALL_MISSING' 'Please install install(1).'
  type -p mktemp > /dev/null || fail 'E_MKTEMP_MISSING' 'Please install mktemp(1).'
}

assert_uid_zero() {
  uid="$(id -u)"
  if [ "$uid" != 0 ]; then
    fail 'E_UID_NONZERO' "dolt install.sh must run as root; please try running with sudo or running\n\`curl $INSTALL_URL | sudo bash\`."
  fi
}

create_workdir() {
  WORK_DIR="$(mktemp -d -t dolt-installer.XXXXXX)"
  cleanup() {
    rm -rf "$WORK_DIR"
  }

  trap cleanup EXIT
  cd "$WORK_DIR"
}

install_binary_release() {
  local FILE="dolt-$PLATFORM_TUPLE.tar.gz"
  local URL="$RELEASES_BASE_URL/$FILE"

  echo "Downloading: $URL"
  curl -A "$CURL_USER_AGENT" -fsL "$URL" > "$FILE"
  tar zxf "$FILE"

  echo 'Installing dolt into /usr/local/bin.'
  [ ! -d /usr/local/bin ] && install -o 0 -g 0 -d /usr/local/bin
  install -o 0 -g 0 "dolt-$PLATFORM_TUPLE/bin/dolt" /usr/local/bin
  install -o 0 -g 0 -d /usr/local/share/doc/dolt/
  install -o 0 -g 0 -m 644 "dolt-$PLATFORM_TUPLE/LICENSES" /usr/local/share/doc/dolt/
}

assert_linux_or_macos
assert_dependencies
assert_uid_zero
create_workdir
install_binary_release

}

_ "$0" "$@"
