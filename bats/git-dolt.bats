#!/usr/bin/env bats

REMOTE=http://localhost:50051/test-org/test-repo

setup() { 
	if [ -z "$BATS_TMPDIR" ]; then
		export BATS_TMPDIR=$HOME/batstmp/
		mkdir $BATS_TMPDIR
	fi

	export PATH=$PATH:$GOPATH/bin
	export NOMS_VERSION_NEXT=1
	cd $BATS_TMPDIR
	mkdir remotes-$$
	echo remotesrv log available here $BATS_TMPDIR/remotes-$$/remotesrv.log
	remotesrv --http-port 1234 --dir ./remotes-$$ &> ./remotes-$$/remotesrv.log 3>&- &
	mkdir dolt-repo-$$
	cd dolt-repo-$$
	dolt init
	dolt remote add test-remote $REMOTE
	dolt push test-remote master
	export DOLT_HEAD_COMMIT=`dolt log -n 1 | grep commit | cut -c 8-`
}

teardown() {
	rm -rf $BATS_TMPDIR/{git,dolt}-repo-$$
	pkill -2 remotesrv
	rm -rf $BATS_TMPDIR/remotes-$$
}

@test "git dolt link takes a remote url (and an optional revspec and destination directory), clones the repo, and outputs a pointer file" {
	mkdir ../git-repo-$$
	cd ../git-repo-$$
	git init
	run git dolt link $REMOTE
	[ "$status" -eq 0 ]
	# Ensure it reports the resolved revision
	[[ "$output" =~ "revision $DOLT_HEAD_COMMIT" ]] || false
	# Ensure it reports the pointer filename
	[[ "$output" =~ "test-repo.git-dolt" ]] || false
	# Ensure it reports the addition to .gitignore
	[[ "$output" =~ "test-repo added to .gitignore" ]] || false
	[ -d test-repo ]

	run cat test-repo.git-dolt
	[[ "${lines[0]}" =~ "version 0" ]] || false
	[[ "${lines[1]}" =~ "remote $REMOTE" ]] || false
	[[ "${lines[2]}" =~ "$DOLT_HEAD_COMMIT" ]] || false

	run cat .gitignore
	[[ "${lines[0]}" =~ "test-repo" ]] || false
}

@test "git dolt fetch takes the name of a git-dolt pointer file and clones the repo to the specified revision if it doesn't exist" {
	mkdir ../git-repo-$$
	cd ../git-repo-$$
	git init

	cat <<EOF > test-repo.git-dolt
version 0
remote $REMOTE
revision $DOLT_HEAD_COMMIT
EOF

	run git dolt fetch test-repo
	[ "$status" -eq 0 ]
	[[ "${lines[0]}" =~ "Dolt repository cloned from remote $REMOTE to directory test-repo at revision $DOLT_HEAD_COMMIT" ]] || false
	[ -d test-repo ]

	cd test-repo
	[ `dolt log -n 1 | grep commit | cut -c 8-` = "$DOLT_HEAD_COMMIT" ]
}

@test "git dolt fails helpfully when dolt is not installed" {
	mkdir TMP_PATH
	pushd TMP_PATH
	which git | xargs ln -sf
	which git-dolt | xargs ln -sf
	popd
	PATH=`pwd`/TMP_PATH run git dolt
	rm -rf TMP_PATH
	[ "$status" -eq 1 ]
	[[ "$output" =~ "It looks like Dolt is not installed on your system" ]]
}

@test "git dolt errors on unknown commands" {
	run git dolt nonsense
	[ "$status" -eq 1 ]
	[[ "$output" =~ "Unknown command" ]] || false
}

@test "git dolt prints usage information with no arguments" {
	run git dolt
	[[ "$output" =~ Usage ]] || false
}