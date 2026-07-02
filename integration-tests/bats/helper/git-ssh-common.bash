load "$BATS_TEST_DIRNAME/helper/query-server-common.bash"

# Helpers for testing dolt against a local bare git repository.
# setup_git_repo                          bare remote seeded on main (no SSH)
# setup_git_ssh_wrapper                   SSH transport via a local wrapper
# setup_git_sshd                          SSH transport via a real sshd
# setup_git_wrapper                       fake git on PATH for observing local
#                                         plumbing such as cat-file
# Each setup has a matching teardown_git_* below. From the bats teardown() hook
# call the ones your tests used.
#
# Each setup function sets the globals below.

# setup_git_repo:
GIT_REMOTE_DIR=""         # path to the bare remote repository

# setup_git_ssh_wrapper:
GIT_SSH_WRAPPER=""        # the GIT_SSH_COMMAND script
GIT_SSH_HOOKS_DIR=""      # snippets sourced per SSH transport call

# setup_git_sshd:
SSHD_DIR=""               # working dir (config, host key, log)
SSHD_PID=""               # pid of the running sshd
SSHD_PORT=""              # loopback TCP port sshd listens on

# setup_git_wrapper:
GIT_WRAPPER_DIR=""        # dir holding the fake git and its tallies
GIT_WRAPPER_HOOKS_DIR=""  # snippets sourced per git invocation
GIT_WRAPPER_SAVED_PATH="" # PATH before the fake git was prepended
GIT_WRAPPER_REAL_GIT=""   # absolute path to the real git binary

# setup_git_repo initializes a bare git repository seeded with one commit on
# main.
setup_git_repo() {
    local parent
    parent="$(mktemp -d "$BATS_TEST_TMPDIR/git-remote.XXXXXX")"
    GIT_REMOTE_DIR="$parent/repo.git"
    git init --bare "$GIT_REMOTE_DIR" >/dev/null

    # Seed via a local path push so the SSH transport is not involved during setup.
    local seed
    seed="$(mktemp -d "$BATS_TEST_TMPDIR/git-remote-seed.XXXXXX")"
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
        git push "$GIT_REMOTE_DIR" main >/dev/null
    )
    rm -rf "$seed"
}

# teardown_git_repo removes the bare remote repository from setup_git_repo.
teardown_git_repo() {
    [[ -n "$GIT_REMOTE_DIR" ]] && rm -rf "${GIT_REMOTE_DIR%/*}"
    GIT_REMOTE_DIR=""
}

# setup_git_ssh_wrapper installs a local SSH transport that routes git commands
# to the bare repository without a running SSH daemon. A hooks directory is
# created so hook_* helpers can inject behavior per invocation.
setup_git_ssh_wrapper() {
    GIT_SSH_HOOKS_DIR="$(mktemp -d "$BATS_TEST_TMPDIR/git-ssh-hooks.XXXXXX")"
    GIT_SSH_WRAPPER="$(mktemp "$BATS_TEST_TMPDIR/git-ssh-wrapper.XXXXXX")"

    cat > "$GIT_SSH_WRAPPER" <<'WRAPPER'
#!/usr/bin/env bash
set -euo pipefail
# nullglob prevents the glob from expanding to a literal string when the hooks
# directory is empty, which would cause the loop to source a nonexistent file.
shopt -s nullglob
for _hook in "${GIT_SSH_HOOKS_DIR}"/*; do
    . "$_hook"
done
# git passes the transport command as the last argument; evaluate it to serve
# the bare repository locally without a running SSH daemon.
eval "${@: -1}"
WRAPPER

    chmod +x "$GIT_SSH_WRAPPER"
    export GIT_SSH_COMMAND="$GIT_SSH_WRAPPER"
    export GIT_SSH_HOOKS_DIR
}

# hook_git_ssh_record_env records an environment variable's value on each SSH
# transport invocation. The value is written to ${BATS_TEST_TMPDIR}/git_env_<var>.
# Must be called after setup_git_ssh_wrapper.
# Arguments:
#   $1  name of the environment variable to record
hook_git_ssh_record_env() {
    local env_var="$1"
    cat > "$GIT_SSH_HOOKS_DIR/env-${env_var}" <<EOF
printf '%s' "\${${env_var}}" > "\${BATS_TEST_TMPDIR}/git_env_${env_var}"
EOF
}

# setup_git_sshd starts a real OpenSSH daemon on a random loopback port.
# SSHD_PORT is set to the port the daemon is listening on.
# Skips the test if sshd is not found on the system.
setup_git_sshd() {
    local sshd_bin
    sshd_bin="$(command -v sshd 2>/dev/null)" || sshd_bin="/usr/sbin/sshd"

    SSHD_DIR="$(mktemp -d "$BATS_TEST_TMPDIR/sshd.XXXXXX")"
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

# teardown_git_ssh tears down the SSH transport: the wrapper from
# setup_git_ssh_wrapper and any real sshd from setup_git_sshd.
teardown_git_ssh() {
    unset GIT_SSH_COMMAND GIT_SSH_HOOKS_DIR SSH_AUTH_SOCK

    [[ -n "$GIT_SSH_WRAPPER" ]]   && rm -f  "$GIT_SSH_WRAPPER"
    [[ -n "$GIT_SSH_HOOKS_DIR" ]] && rm -rf "$GIT_SSH_HOOKS_DIR"
    rm -f "$BATS_TEST_TMPDIR"/git_env_*
    GIT_SSH_WRAPPER=""
    GIT_SSH_HOOKS_DIR=""

    if [[ -n "${SSHD_PID:-}" ]]; then
        kill "$SSHD_PID" 2>/dev/null || true
        wait "$SSHD_PID" 2>/dev/null || true
        SSHD_PID=""
    fi
    [[ -n "${SSHD_DIR:-}" ]] && rm -rf "$SSHD_DIR"
    SSHD_DIR=""
    SSHD_PORT=""
}

# setup_git_wrapper puts a fake git first on PATH that sources every snippet in
# GIT_WRAPPER_HOOKS_DIR on each invocation and then runs the real git. It is the
# only way to observe local plumbing such as cat-file. teardown_git_wrapper
# undoes it.
setup_git_wrapper() {
    GIT_WRAPPER_DIR="$(mktemp -d "$BATS_TEST_TMPDIR/git-wrapper.XXXXXX")"
    GIT_WRAPPER_HOOKS_DIR="$GIT_WRAPPER_DIR/hooks"
    GIT_WRAPPER_SAVED_PATH="$PATH"
    mkdir -p "$GIT_WRAPPER_HOOKS_DIR"
    GIT_WRAPPER_REAL_GIT="$(command -v git)"
    local real_git="$GIT_WRAPPER_REAL_GIT"
    # Hooks are sourced with the git argv in scope.
    cat > "$GIT_WRAPPER_DIR/git" << WRAPPER
#!/usr/bin/env bash
# No set -e here: a failing hook must not stop the real git from running.
shopt -s nullglob
for _hook in "$GIT_WRAPPER_HOOKS_DIR"/*; do
    . "\$_hook"
done
exec "$real_git" "\$@"
WRAPPER
    chmod +x "$GIT_WRAPPER_DIR/git"
    export PATH="$GIT_WRAPPER_DIR:$PATH"
}

# hook_git_count_subcommand registers a hook that records one byte each time the
# given git subcommand runs under setup_git_wrapper. Read the total with
# git_subcommand_count. Dolt passes the repository through GIT_DIR rather than a
# flag, so the subcommand is always the first argument and matching it exactly
# keeps a path or ref named like the subcommand out of the count.
# Arguments:
#   $1  git subcommand to count, e.g. cat-file
hook_git_count_subcommand() {
    local subcommand="$1"
    cat > "$GIT_WRAPPER_HOOKS_DIR/count-${subcommand}" << HOOK
if [[ "\$1" == "${subcommand}" ]]; then
    printf '.' >> "$GIT_WRAPPER_DIR/count-${subcommand}"
fi
HOOK
}

# git_subcommand_count prints how many times the given subcommand ran under
# setup_git_wrapper, or zero if it never ran.
# Arguments:
#   $1  git subcommand to read the count for
git_subcommand_count() {
    local subcommand="$1"
    local tally_file="$GIT_WRAPPER_DIR/count-${subcommand}"
    if [[ -f "$tally_file" ]]; then
        # Arithmetic expansion drops any whitespace wc may pad, so the count is
        # a bare integer usable by string-equality callers, not only by -lt.
        echo "$(( $(wc -c < "$tally_file") ))"
    else
        echo 0
    fi
}

# teardown_git_wrapper restores PATH and removes the git binary wrapper from
# setup_git_wrapper.
teardown_git_wrapper() {
    [[ -n "$GIT_WRAPPER_SAVED_PATH" ]] && export PATH="$GIT_WRAPPER_SAVED_PATH"
    [[ -n "$GIT_WRAPPER_DIR" ]] && rm -rf "$GIT_WRAPPER_DIR"
    GIT_WRAPPER_DIR=""
    GIT_WRAPPER_HOOKS_DIR=""
    GIT_WRAPPER_SAVED_PATH=""
    GIT_WRAPPER_REAL_GIT=""
}

# largest_data_blob_size prints the size in bytes of the largest blob in a git
# directory. For a Dolt git remote that is the table file, a cheap proxy for how
# many chunks the remote holds. It runs the real git directly, so it works
# whether or not setup_git_wrapper is installed, and its reads are not counted.
# Arguments:
#   $1  path to the git directory to inspect
largest_data_blob_size() {
    local git_dir="$1"
    "${GIT_WRAPPER_REAL_GIT:-git}" --git-dir "$git_dir" \
        cat-file --batch-all-objects --batch-check \
        | awk '$2 == "blob" && $3 > max { max = $3 } END { if (max != "") print max }'
}
