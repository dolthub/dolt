# ResolveAddr extracted from net

Extracted `ResolveAddr` code from golang `net` package.

This should never have had to happen.

Removed DNS resolution-- i didnt need it and it was 2x more work.
PRs accepted.
