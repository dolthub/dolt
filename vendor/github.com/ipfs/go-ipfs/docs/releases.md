# go-ipfs releases

## Release Schedule
go-ipfs is on a six week release schedule. Following a release, there will be
five weeks for code of any type (features, bugfixes, etc) to be added. After
the five weeks is up, a release canidate is tagged and only important bugfixes
will be allowed up to release day.

## Pre-Release Checklist
- [ ] before release, tag 'release canidate' for users to test against
  - if bugs are found/fixed, do another release canidate
- [ ] all tests pass (no exceptions)
- [ ] webui works (for most definitions of 'works')
- [ ] CHANGELOG.md has been updated
  - use `LAST=v0.4.2 ; for n in $(git log --oneline --merges --reverse -n-1 $LAST...master | cut -d'#' -f2 | cut -d' ' -f1); do echo https://github.com/ipfs/go-ipfs/pull/$n; done`
- [ ] version string in `repo/config/version.go` has been updated
- [ ] tag commit with vX.Y.Z
- [ ] bump version string in `repo/config/version.go` to `vX.Y.Z-dev`
- [ ] update release branch to point to release commit
- [ ] publish dist.ipfs.io
- [ ] publish next version to https://github.com/ipfs/npm-go-ipfs

## Post-Release
- Communication
  - [ ] Create the release issue
  - [ ] Announcements (both pre-release and post-release)
    - [ ] Twitter
    - [ ] IRC
    - [ ] Reddit
  - [ ] Blog post (at minimum, paste the changelog. optionally add context and thank contributors.)
- [ ] Update HTTP-API Documentation on the Website using https://github.com/ipfs/http-api-docs
