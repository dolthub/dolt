load "$BATS_TEST_DIRNAME/helper/query-server-common.bash"

# Helpers for testing dolt against a local bare git repository over SSH.
# setup_git_repo + setup_git_ssh_wrapper: wrapper transport (env-var tests)
# setup_git_repo + setup_git_sshd:        real OpenSSH daemon (auth tests)
# teardown_git cleans up all resources; call from the bats teardown() hook.
#
# Variables set by the setup functions:
#   GIT_SVC_DIR   path to the bare repository
#   GIT_SVC_HOOKS_DIR  directory sourced before each wrapper invocation
#   SSHD_PORT     TCP port the real sshd listens on (set by setup_git_sshd)
GIT_SVC_DIR=""
GIT_SVC_WRAPPER=""
GIT_SVC_HOOKS_DIR=""
SSHD_DIR=""
SSHD_PID=""
SSHD_PORT=""

# setup_git_repo initializes a bare git repository seeded with one commit on main.
setup_git_repo() {
    local parent
    parent="$(mktemp -d "$BATS_TMPDIR/git-svc.XXXXXX")"
    GIT_SVC_DIR="$parent/repo.git"
    git init --bare "$GIT_SVC_DIR" >/dev/null

    # Seed via a local path push so the SSH transport is not involved during setup.
    local seed
    seed="$(mktemp -d "$BATS_TMPDIR/git-svc-seed.XXXXXX")"
    (
        set -euo pipefail
        cd "$seed"
        git init >/dev/null
        git config user.email "bats@email.fake"
        git config user.name "Bats Tests"
        echo "seed" > README
        git add README
        git commit -m "seed" >/dev/null
        git branch -M main
        git push "$GIT_SVC_DIR" main >/dev/null
    )
    rm -rf "$seed"
}

# setup_git_ssh_wrapper installs a local SSH transport that routes git commands
# to the bare repository without a running SSH daemon. A hooks directory is
# created so hook_* helpers can inject behavior per invocation.
setup_git_ssh_wrapper() {
    GIT_SVC_HOOKS_DIR="$(mktemp -d "$BATS_TMPDIR/git-svc-hooks.XXXXXX")"
    GIT_SVC_WRAPPER="$(mktemp "$BATS_TMPDIR/git-svc-wrapper.XXXXXX")"

    cat > "$GIT_SVC_WRAPPER" <<'WRAPPER'
#!/usr/bin/env bash
set -euo pipefail
# nullglob prevents the glob from expanding to a literal string when the hooks
# directory is empty, which would cause the loop to source a nonexistent file.
shopt -s nullglob
for _h in "${GIT_SVC_HOOKS_DIR}"/*; do
    . "$_h"
done
# git passes the transport command as the last argument; evaluate it to serve
# the bare repository locally without a running SSH daemon.
eval "${@: -1}"
WRAPPER

    chmod +x "$GIT_SVC_WRAPPER"
    export GIT_SSH_COMMAND="$GIT_SVC_WRAPPER"
    export GIT_SVC_HOOKS_DIR
}

# setup_git_sshd starts a real OpenSSH daemon on a random loopback port.
# SSHD_PORT is set to the port the daemon is listening on.
# Skips the test if sshd is not found on the system.
setup_git_sshd() {
    local sshd_bin
    sshd_bin="$(command -v sshd 2>/dev/null)" || sshd_bin="/usr/sbin/sshd"

    SSHD_DIR="$(mktemp -d "$BATS_TMPDIR/sshd.XXXXXX")"
    chmod 700 "$SSHD_DIR"

    ssh-keygen -q -t ed25519 -f "$SSHD_DIR/host_key" -N ""
    chmod 600 "$SSHD_DIR/host_key"

    touch "$SSHD_DIR/authorized_keys"
    chmod 600 "$SSHD_DIR/authorized_keys"

    SSHD_PORT="$(definePORT)"

    cat > "$SSHD_DIR/sshd_config" <<EOF
Port ${SSHD_PORT}
ListenAddress 127.0.0.1
HostKey $SSHD_DIR/host_key
AuthorizedKeysFile $SSHD_DIR/authorized_keys
StrictModes no
PasswordAuthentication no
UsePAM no
UsePrivilegeSeparation no
AllowTcpForwarding no
X11Forwarding no
LogLevel ERROR
EOF

    # Redirect stdio to /dev/null so sshd does not inherit bats' open pipe
    # file descriptors. Without this, a running sshd would hold those pipes
    # open and prevent bats from reaching EOF after all tests complete.
    "$sshd_bin" -f "$SSHD_DIR/sshd_config" -D \
        </dev/null >>"$SSHD_DIR/sshd.log" 2>&1 &
    SSHD_PID=$!

    local i
    for (( i = 0; i < 50; i++ )); do
        # /dev/tcp is a bash built-in that avoids a dependency on netcat.
        (: >/dev/tcp/localhost/"$SSHD_PORT") 2>/dev/null && return 0
        sleep 0.1
    done
    echo "sshd failed to start on port $SSHD_PORT" >&2
    return 1
}

# gen_ssh_key generates an ed25519 key pair and authorizes it with the test sshd.
# Must be called after setup_git_sshd.
# Arguments:
#   $1  path for the private key
#   $2  passphrase (empty string for an unprotected key)
gen_ssh_key() {
    local keypath="$1"
    local passphrase="$2"
    rm -f "$keypath" "${keypath}.pub"
    ssh-keygen -q -t ed25519 -f "$keypath" -N "$passphrase"
    cat "${keypath}.pub" >> "$SSHD_DIR/authorized_keys"
}

# teardown_git removes all resources created by the setup_git_* functions.
# Safe to call even when only some setup functions were called.
teardown_git() {
    unset GIT_SSH_COMMAND GIT_SVC_HOOKS_DIR SSH_AUTH_SOCK

    [[ -n "$GIT_SVC_DIR" ]]       && rm -rf "${GIT_SVC_DIR%/*}"
    [[ -n "$GIT_SVC_WRAPPER" ]]   && rm -f  "$GIT_SVC_WRAPPER"
    [[ -n "$GIT_SVC_HOOKS_DIR" ]] && rm -rf "$GIT_SVC_HOOKS_DIR"
    rm -f "$BATS_TMPDIR"/git_env_*

    GIT_SVC_DIR=""
    GIT_SVC_WRAPPER=""
    GIT_SVC_HOOKS_DIR=""

    if [[ -n "${SSHD_PID:-}" ]]; then
        kill "$SSHD_PID" 2>/dev/null || true
        wait "$SSHD_PID" 2>/dev/null || true
        SSHD_PID=""
    fi

    [[ -n "${SSHD_DIR:-}" ]] && rm -rf "$SSHD_DIR"
    SSHD_DIR=""
    SSHD_PORT=""
}

# hook_git_record_env records an environment variable's value on each git
# transport invocation. The value is written to ${BATS_TMPDIR}/git_env_<var>.
# Must be called after setup_git_ssh_wrapper.
# Arguments:
#   $1  name of the environment variable to record
hook_git_record_env() {
    local var="$1"
    cat > "$GIT_SVC_HOOKS_DIR/env-${var}" <<EOF
printf '%s' "\${${var}}" > "\${BATS_TMPDIR}/git_env_${var}"
EOF
}
