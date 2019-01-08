#!/bin/bash
set -e

# see also ".mailmap" for how email addresses and names are deduplicated


cat >AUTHORS <<-'EOF'
# This file lists all individuals having contributed content to the repository.
# For how it is generated, see `docs/generate-authors.sh`.

EOF
git log --format='%aN <%aE>' | LC_ALL=C.UTF-8 sort -uf >>AUTHORS
