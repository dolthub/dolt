#!/bin/sh
# Fake $EDITOR for sql-shell-edit-delimiter.expect: replaces the \edit buffer
# with SQL containing an embedded DELIMITER line.
printf 'DELIMITER |\nselect 42|' > "$1"
