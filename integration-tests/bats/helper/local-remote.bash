load helper/query-server-common

SKIP_SERVER_TESTS=$(cat <<-EOM
~sql-spatial-types.bats~
~column_tags.bats~
~migration-integration.bats~
~sql.bats~
~import-update-tables.bats~
~sql-conflicts.bats~
~chunk-journal.bats~
~2pk5cols-ints.bats~
~no-repo.bats~
~sql-show.bats~
~schema-import.bats~
~sql-create-database.bats~
~conflict-detection-2.bats~
~remotes-aws.bats~
~replace.bats~
~remotes-sql-server.bats~
~foreign-keys.bats~
~sql-local-remote.bats~
~1pk5col-strings.bats~
~sql-tags.bats~
~empty-repo.bats~
~clone-drops.bats~
~verify-constraints.bats~
~db-revision-specifiers.bats~
~ignore.bats~
~validation.bats~
~remotes-file-system.bats~
~sql-charsets-collations.bats~
~sql-cherry-pick.bats~
~sql-local-remote.bats~
~primary-key-changes.bats~
~common.bash.bats~
~remotes-localbs.bats~
~conflict-detection.bats~
~json.bats~
~copy-tags.bats~
~cp-and-mv.bats~
~sql-create-tables.bats~
~sql-multi-db.bats~
~blame-system-view.bats~
~import-mysqldump.bats~
~1pksupportedtypes.bats~
~init.bats~
~index.bats~
~merge.bats~
~1pk5col-ints.bats~
~sql-client.bats~
~sql-status.bats~
~window.bats~
~import-create-tables.bats~
~status.bats~
~import-replace-tables.bats~
~show.bats~
~system-tables.bats~
~deleted-branches.bats~
~schema-export.bats~
~sql-reserved-column-name.bats~
~dump-docs.bats~
~tableplus.bats~
~multidb.bats~
~sql-conflicts-resolve.bats~
~conflicts-resolve.bats~
~export-tables.bats~
~filter-branch.bats~
~arg-parsing.bats~
~dump.bats~
~rename-tables.bats~
~sql-backup.bats~
~drop-create.bats~
~constraint-violations.bats~
~branch-control.bats~
~import-tables.bats~
~feature-version.bats~
~sql-server.bats~
~merge-3way-schema-changes.bats~
~sql-server-remotesrv.bats~
~large-update.bats~
~remotes.bats~
~remotes-push-pull.bats~
~create-views.bats~
~blame.bats~
~multiple-tables.bats~
~json-new-fmt.bats~
~replication-multidb.bats~
~spatial-index.bats~
~schema-changes.bats~
~replication.bats~
~docs.bats~
~remotesrv.bats~
~case-sensitivity.bats~
~garbage_collection.bats~
~diff-stat.bats~
~sql-merge.bats~
~import-append-tables.bats~
~backup.bats~
~default-values.bats~
~types.bats~
~config-home.bats~
~cherry-pick.bats~
~triggers.bats~
~config.bats~
~sql-branch.bats~
~sql-check-constraints.bats~
~keyless.bats~
~regression-tests.bats~
~sql-privs.bats~
~keyless-foreign-keys.bats~
~index-on-writes-2.bats~
~checkout.bats~
~sql-fetch.bats~
~foreign-keys-invert-pk.bats~
~merge-base.bats~
~auto_increment.bats~
~creds.bats~
~schema-conflicts.bats~
~sql-diff.bats~
~index-on-writes.bats~
~migrate.bats~
~sql-load-data.bats~
~conflict-cat.bats~
~sql-config.bats~
~sql-add.bats~
~doltpy.bats~
~sql-batch.bats~
~send-metrics.bats~
~commit.bats~
~sql-commit.bats~
~reset.bats~
~sql-reset.bats~
~sql-checkout.bats~
~cli-hosted.bats~
~profile.bats~
~ls.bats~
~rebase.bats~
~shallow-clone.bats~
~archive.bats~
~fsck.bats~
~createchunk.bats~
~import-no-header-csv.bats~
~import-no-header-psv.bats~
~admin-conjoin.bats~
EOM
)

# Starts a remote server in the current working directory for the
# purposes of running dolt commands against a running sql-server.
setup_remote_server() {
  script_name=$(basename ${BATS_TEST_FILENAME})
  if [ "$SQL_ENGINE" = "remote-engine" ];
  then
    echo "Using remote engine for tests" >& 3
    if [[ "$SKIP_SERVER_TESTS" =~ "~$script_name~" ]];
    then
      skip
    else
      SQL_USER=root
      start_sql_server
    fi
  fi
}

teardown_remote_server() {
  if [ "$SQL_ENGINE" = "remote-engine" ];
  then
    stop_sql_server
  fi
}

skip_if_remote() {
  if [ "$SQL_ENGINE" = "remote-engine" ];
  then
    skip
  fi
}