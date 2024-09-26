#!/bin/bash

set -e

if [[ $# -ne 1 ]]; then
  echo "Usage: ./validate-commentor.sh COMMENTOR"
  exit 1
fi

validcommentors="coffeegoddd andy-wm-arthur bheni Hydrocharged reltuk tbantle22 timsehn zachmu max-hoffman nicktobey fulghum macneale4 jycor"

contains() {
    [[ $1 =~ (^|[[:space:]])$2($|[[:space:]]) ]] && echo "valid=true" >> $GITHUB_OUTPUT || exit 0
}

contains "$validcommentors" "$1"
