#!/bin/bash

set -e

if [[ $# -ne 1 ]]; then
  echo "Usage: ./validate-commentor.sh COMMENTOR"
  exit 1
fi

validcommentors="coffeegoddd andrew-wm-arthur bheni Hydrocharged reltuk tbantle22 timsehn VinaiRachakonda zachmu max-hoffman"

contains() {
    [[ $1 =~ (^|[[:space:]])$2($|[[:space:]]) ]] && echo "::set-output name=valid::true" || exit 0
}

contains "$validcommentors" "$1"
