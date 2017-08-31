#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'
set -x

auth=""
#auth="-u kubuxu:$GH_TOKEN"
org=ipfs
repo=go-ipfs
arch_repo=go-ipfs-archived
api_repo="repos/$org/$repo"

exclusions=(
	'master'
	'release'
	'feat/zcash'
)

gh_api_next() {
	links=$(grep '^Link:' | sed -e 's/Link: //' -e 's/, /\n/g')
	echo "$links" | grep '; rel="next"' >/dev/null || return
	link=$(echo "$links" | grep '; rel="next"' | sed -e 's/^<//' -e 's/>.*//')

	curl $auth -f -sD >(gh_api_next) "$link"
}

gh_api() {
	curl $auth -f -sD >(gh_api_next) "https://api.github.com/$1" | jq -s '[.[] | .[]]'
}

pr_branches() {
	gh_api "$api_repo/pulls" |  jq -r '.[].head.label | select(test("^ipfs:"))' \
		| sed 's/^ipfs://'
}

origin_refs() {
	format=${1-'%(refname:short)'}

	git for-each-ref --format "$format" refs/remotes/origin | sed 's|^origin/||'
}

active_branches() {
	origin_refs '%(refname:short) %(committerdate:unix)' |awk \
'	BEGIN { monthAgo = systime() - 31*24*60*60 }
	{ if ($2 > monthAgo) print $1 }
'
}

git remote add archived "git@github.com:$org/$arch_repo.git" || true

cat <(active_branches) <(pr_branches) <((IFS=$'\n'; echo "${exclusions[*]}")) \
	| sort -u | comm - <(origin_refs | sort) -13 |\
	while read -r ref; do
		git push archived "origin/$ref:refs/heads/$ref/$(date --rfc-3339=date)"
		git push origin --delete "$ref"
	done

