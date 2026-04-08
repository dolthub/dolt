load $BATS_TEST_DIRNAME/helper/query-server-common.bash

# Helpers for testing dolt against a local bare git repository over SSH.
# Functions compose in two layers:
#
#   1. setup_git_repo        - bare repository
#   2. setup_git_ssh_wrapper - shell wrapper for GIT_SSH_COMMAND (no real sshd, for env tests)
#      setup_git_sshd        - real OpenSSH daemon (for authentication tests)
#
# Typical usage for environment variable propagation tests:
#   setup_git_repo
#   setup_git_ssh_wrapper
#   dolt remote add origin "git@localhost:${GIT_SVC_DIR}"
#   hook_git_record_env GIT_SSH_COMMAND
#   teardown_git
#
# Typical usage for real SSH authentication tests:
#   setup_git_repo
#   setup_git_sshd
#   gen_ssh_key "$BATS_TMPDIR/mykey" ""          # unlocked key
#   export GIT_SSH_COMMAND="ssh -i $BATS_TMPDIR/mykey -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
#   dolt remote add origin "git+ssh://$(whoami)@127.0.0.1:${SSHD_PORT}${GIT_SVC_DIR}"
#   teardown_git
#
# teardown_git cleans up all resources regardless of which setup functions were called.
#
# Variables set by the setup functions:
#   GIT_SVC_DIR       path to the bare repository
#   GIT_SVC_WRAPPER   path to the GIT_SSH_COMMAND wrapper script
#   GIT_SVC_HOOKS_DIR path to the directory sourced before each transport invocation
#   SSHD_PORT         TCP port the real sshd is listening on (set by setup_git_sshd)
GIT_SVC_DIR=""
GIT_SVC_WRAPPER=""
GIT_SVC_HOOKS_DIR=""
SSHD_DIR=""
SSHD_PID=""
SSHD_PORT=""

# setup_git_repo initializes a bare git repository seeded with one commit on main.
setup_git_repo() {
    local _parent
    _parent="$(mktemp -d "$BATS_TMPDIR/git-svc.XXXXXX")"
    GIT_SVC_DIR="$_parent/repo.git"
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
# nullglob prevents the glob from expanding to a literal path when the hooks
# directory is empty, which would cause the loop to source a nonexistent file.
shopt -s nullglob
for _h in "${GIT_SVC_HOOKS_DIR}"/*; do
    . "$_h"
done
# git passes the transport command as the last argument, so evaluating it here
# serves the bare repository locally without a running SSH daemon.
eval "${@: -1}"
WRAPPER
    chmod +x "$GIT_SVC_WRAPPER"
    export GIT_SSH_COMMAND="$GIT_SVC_WRAPPER"
    export GIT_SVC_HOOKS_DIR
}

# setup_git_sshd starts a real OpenSSH daemon on a random loopback port.
# After this call, SSHD_PORT is the port the daemon is listening on and
# SSHD_DIR holds its config and host key. Call gen_ssh_key to authorize a
# client key before making SSH connections. Skips the test if sshd is not found.
setup_git_sshd() {
    local sshd_bin
    sshd_bin="$(command -v sshd 2>/dev/null || true)"
    [[ -z "$sshd_bin" && -x /usr/sbin/sshd ]] && sshd_bin=/usr/sbin/sshd
    if [[ -z "$sshd_bin" ]]; then
        skip "sshd not found"
    fi

    SSHD_DIR="$(mktemp -d "$BATS_TMPDIR/sshd.XXXXXX")"
    chmod 700 "$SSHD_DIR"

    ssh-keygen -q -t ed25519 -f "$SSHD_DIR/host_key" -N ""
    chmod 600 "$SSHD_DIR/host_key"

    touch "$SSHD_DIR/authorized_keys"
    chmod 600 "$SSHD_DIR/authorized_keys"

    SSHD_PORT="$(definePORT)"

    # Windows OpenSSH reads config paths as native Windows paths and does not
    # support UsePAM, so we convert with cygpath and omit the PAM directive.
    local host_key="$SSHD_DIR/host_key"
    local auth_keys="$SSHD_DIR/authorized_keys"
    local pid_file="$SSHD_DIR/sshd.pid"
    local use_pam="UsePAM no"
    if [[ "$IS_WINDOWS" == true ]] && command -v cygpath >/dev/null 2>&1; then
        host_key="$(cygpath -w "$host_key")"
        auth_keys="$(cygpath -w "$auth_keys")"
        pid_file="$(cygpath -w "$pid_file")"
        use_pam=""
    fi

    cat > "$SSHD_DIR/sshd_config" <<EOF
Port ${SSHD_PORT}
ListenAddress 127.0.0.1
HostKey ${host_key}
AuthorizedKeysFile ${auth_keys}
StrictModes no
PasswordAuthentication no
${use_pam}
AllowTcpForwarding no
X11Forwarding no
PidFile ${pid_file}
LogLevel ERROR
EOF

    if command -v sudo >/dev/null 2>&1; then
        sudo "$sshd_bin" -f "$SSHD_DIR/sshd_config" -D &
    else
        "$sshd_bin" -f "$SSHD_DIR/sshd_config" -D &
    fi
    SSHD_PID=$!

    local i
    for i in $(seq 1 50); do
        # Use bash /dev/tcp rather than nc so the check works on Windows.
        if (: >/dev/tcp/localhost/"$SSHD_PORT") 2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    echo "sshd failed to start on port $SSHD_PORT" >&2
    return 1
}

# gen_ssh_key |keypath| |passphrase|
# Generates an ed25519 key pair at |keypath| and appends the public key to the
# sshd's authorized_keys file. Must be called after setup_git_sshd.
gen_ssh_key() {
    local keypath="$1"
    local passphrase="$2"
    rm -f "$keypath" "${keypath}.pub"
    ssh-keygen -q -t ed25519 -f "$keypath" -N "$passphrase"
    cat "${keypath}.pub" >> "$SSHD_DIR/authorized_keys"
}

# teardown_git removes all resources created by the setup_git_* functions.
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
        if command -v sudo >/dev/null 2>&1; then
            sudo kill "$SSHD_PID" 2>/dev/null || true
        else
            kill "$SSHD_PID" 2>/dev/null || true
        fi
        wait "$SSHD_PID" 2>/dev/null || true
        SSHD_PID=""
    fi
    [[ -n "${SSHD_DIR:-}" ]] && rm -rf "$SSHD_DIR"
    SSHD_DIR=""
    SSHD_PORT=""
}

# hook_git_record_env records the value of |var| during each git transport
# invocation. The value is written to ${BATS_TMPDIR}/git_env_${var}.
hook_git_record_env() {
    local var="$1"
    printf 'printf "%%s" "${%s}" > "${BATS_TMPDIR}/git_env_%s"\n' "$var" "$var" \
        > "$GIT_SVC_HOOKS_DIR/env-${var}"
}
