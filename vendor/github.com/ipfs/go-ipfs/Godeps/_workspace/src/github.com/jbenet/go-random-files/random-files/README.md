# random-files - create random fs hierarchies

random-files creates random fs hierarchies. Useful for testing filesystems.

## Install

```
go get -u github.com/jbenet/go-random-files/random-files
```

## Usage

```sh
> random-files --help
usage: random-files [options] <path>...
Write a random filesystem hierarchy to each <path>

Options:
  -alphabet="easy": alphabet for filenames {easy, hard}
  -depth=2: fanout depth - how deep the hierarchy goes
  -dirs=5: fanout dirs - number of dirs per dir (or max)
  -files=10: fanout files - number of files per dir (or max
  -filesize=4096: filesize - how big to make each file (or max)
  -q=false: quiet output
  -random-crypto=false: use cryptographic randomness for files
  -random-fanout=false: randomize fanout numbers
  -random-size=true: randomize filesize
  -seed=0: random seed - 0 for current time
```

## Examples

```sh
> random-files --depth=2 --files=3 foo
foo/h20uo3jrpihb
foo/x6tef1
foo/jh0c2vdci
foo/fden012m368
foo/fden012m368/p6n0chy4kg
foo/fden012m368/h92_
foo/fden012m368/kvjiya98p3
foo/e_i6hwav1tb
foo/e_i6hwav1tb/oj0-a
foo/e_i6hwav1tb/1-pfgvim
foo/e_i6hwav1tb/s_unf
foo/bgvy8x-_hsm
foo/bgvy8x-_hsm/98zcoz-9ng
foo/bgvy8x-_hsm/j0see3qv
foo/bgvy8x-_hsm/qntuf0r
foo/6zjkw3ejm2awwt
foo/6zjkw3ejm2awwt/iba52dh1lhnewh
foo/6zjkw3ejm2awwt/n1bwcv5zpe
foo/6zjkw3ejm2awwt/o8k89cc
foo/efp_6
foo/efp_6/qfap2
foo/efp_6/v_kl_wlefsaa
foo/efp_6/r7sdbph
```

It made:

```
> tree foo
foo
├── 6zjkw3ejm2awwt
│   ├── iba52dh1lhnewh
│   ├── n1bwcv5zpe
│   └── o8k89cc
├── bgvy8x-_hsm
│   ├── 98zcoz-9ng
│   ├── j0see3qv
│   └── qntuf0r
├── e_i6hwav1tb
│   ├── 1-pfgvim
│   ├── oj0-a
│   └── s_unf
├── efp_6
│   ├── qfap2
│   ├── r7sdbph
│   └── v_kl_wlefsaa
├── fden012m368
│   ├── h92_
│   ├── kvjiya98p3
│   └── p6n0chy4kg
├── h20uo3jrpihb
├── jh0c2vdci
└── x6tef1

5 directories, 18 files
```
