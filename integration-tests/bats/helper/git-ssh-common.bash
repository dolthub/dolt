# Helpers for testing dolt against a local bare git repository served through
# a simulated SSH transport. Functions compose in three layers:
#
#   1. setup_git_repo        - bare repository
#   2. setup_git_ssh_wrapper - SSH transport with hook injection
#   3. hook_*                - per-test transport behaviors
#
# Typical usage:
#   setup_git_repo
#   setup_git_ssh_wrapper
#   dolt remote add origin "git@localhost:${GIT_SVC_DIR}"
#   hook_git_record_env GIT_SSH_COMMAND
#   teardown_git  # works for any combination of setup_git_* calls
#
# Variables set by the setup functions:
#   GIT_SVC_DIR       path to the bare repository
#   GIT_SVC_WRAPPER   path to the GIT_SSH_COMMAND wrapper script
#   GIT_SVC_HOOKS_DIR path to the directory sourced before each transport invocation
GIT_SVC_DIR=""
GIT_SVC_WRAPPER=""
GIT_SVC_HOOKS_DIR=""

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

# teardown_git removes all resources created by the setup_git_* functions
# and any output files written by hook_* helpers.
teardown_git() {
    unset GIT_SSH_COMMAND GIT_SVC_HOOKS_DIR
    [[ -n "$GIT_SVC_DIR" ]]       && rm -rf "${GIT_SVC_DIR%/*}"
    [[ -n "$GIT_SVC_WRAPPER" ]]   && rm -f  "$GIT_SVC_WRAPPER"
    [[ -n "$GIT_SVC_HOOKS_DIR" ]] && rm -rf "$GIT_SVC_HOOKS_DIR"
    rm -f "$BATS_TMPDIR/git_ssh_passphrase" "$BATS_TMPDIR"/git_env_*
    GIT_SVC_DIR=""
    GIT_SVC_WRAPPER=""
    GIT_SVC_HOOKS_DIR=""
}

# hook_git_record_env records the value of |var| during each git transport
# invocation. The value is written to ${BATS_TMPDIR}/git_env_${var}.
hook_git_record_env() {
    local var="$1"
    printf 'printf "%%s" "${%s}" > "${BATS_TMPDIR}/git_env_%s"\n' "$var" "$var" \
        > "$GIT_SVC_HOOKS_DIR/env-${var}"
}

# hook_git_passphrase_prompt installs a passphrase prompt into the hook chain.
# On the first transport invocation it writes "Enter passphrase: " to /dev/tty,
# reads the response from /dev/tty, and records it to ${BATS_TMPDIR}/git_ssh_passphrase.
# Subsequent invocations skip the prompt.
#
# /dev/tty is used for both the prompt and the read because that is what real
# ssh does. OpenSSH readpass.c opens _PATH_TTY directly:
#
#   ttyfd = open(_PATH_TTY, O_RDWR);   /* _PATH_TTY == "/dev/tty" */
#
# readpassphrase(3) documents: "displays a prompt to, and reads in a passphrase
# from, /dev/tty." RPP_REQUIRE_TTY is set, so there is no stdin fallback.
#
# Git sets conn->in = -1 in connect.c before spawning GIT_SSH_COMMAND, causing
# start_command() to create a pipe for the SSH process stdin. That pipe carries
# git pack-protocol data. /dev/tty and stdin are independent channels.
#
# A passing test confirms that the controlling terminal is reachable through the
# dolt, git, GIT_SSH_COMMAND subprocess chain, the same path a real ssh
# passphrase prompt travels in a live user session.
hook_git_passphrase_prompt() {
    cat > "$GIT_SVC_HOOKS_DIR/passphrase" <<'HOOK'
if [[ ! -f "${BATS_TMPDIR}/git_ssh_passphrase" ]]; then
    printf "Enter passphrase: " >/dev/tty
    read -r passphrase < /dev/tty
    printf "%s" "$passphrase" > "${BATS_TMPDIR}/git_ssh_passphrase"
fi
HOOK
}
