# nomsfs

Nomsfs is a [FUSE](https://en.wikipedia.org/wiki/Filesystem_in_Userspace) filesystem built on Noms. To use it you'll need FUSE:

* *Linux* -- built-in; you should be good to go
* *Mac OS X* -- Install [FUSE for OS X](https://osxfuse.github.io/)

Development and testing have been done exclusively on Mac OS X using FUSE for OS X.
Nomsfs builds on the [Go FUSE imlementation](https://github.com/hanwen/go-fuse) from Han-Wen Nienhuys.

## Usage

Make sure FUSE is installed. On Mac OS X remember to run `/Library/Filesystems/osxfusefs.fs/Support/load_osxfusefs`.


Build with `go build` (or just run with `go run nomsfs.go`); test with `go test`.

Mount an existing or new dataset by executing `nomsfs`:

```
$ mkdir /var/tmp/mnt
$ go run nomsfs.go /var/tmp/nomsfs::fs /var/tmp/mnt
running...
```

Use ^C to stop `nomsfs`

### Exploring The Data

1. Once you have a mount point and `nomsfs` is running you can add/delete/rename files and directories using the Finder or the command line as you would with any other file system.
2. Stop `nomsfs` with ^C
3. Let's look around the dataset:
```
> noms ds /var/tmp/nomsfs
fs
> noms show /var/tmp/nomsfs::fs
struct Commit {
  meta: struct {},
  parents: Set<Ref<Cycle<Commit>>>,
  value: struct Filesystem {
    root: struct Inode {
      attr: struct Attr {
        ctime: Number,
        gid: Number,
        mode: Number,
        mtime: Number,
        uid: Number,
        xattr: Map<String, Blob>,
      },
      contents: struct Directory {
        entries: Map<String, Cycle<1>>,
      } | struct Symlink {
        targetPath: String,
      } | struct File {
        data: Ref<Blob>,
      },
    },
  },
}({
  meta:  {},
  parents: {
    d6jn389ov693oa4b9vqhe3fmn2g49c2k,
  },
  value: Filesystem {
    root: Inode {
      attr: Attr {
        ctime: 1.4703496225642643e+09,
        gid: 20,
        mode: 511,
        mtime: 1.4703496225642643e+09,
        uid: 501,
        xattr: {},
      },
      contents: Directory {
        entries: {
          "file.txt": Inode {
            attr: Attr {
              ctime: 1.470349669044128e+09,
              gid: 20,
              mode: 420,
              mtime: 1.465233596e+09,
              uid: 501,
              xattr: {
                "com.apple.FinderInfo": 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  // 32 B
                00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00,
              },
            },
            contents: File {
              data: hv6f7d07uajec3mebergu810v12gem83,
            },
          },
          "noms_logo.png": Inode {
            attr: Attr {
              ctime: 1.4703496464136713e+09,
              gid: 20,
              mode: 420,
              mtime: 1.470171468e+09,
              uid: 501,
              xattr: {
                "com.apple.FinderInfo": 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  // 32 B
                00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00,
                "com.apple.quarantine": 30 30 30 32 3b 35 37 61 31 30 39 34 63 3b 50 72  // 22 B
                65 76 69 65 77 3b,
              },
            },
            contents: File {
              data: higtjmhq7fo5m072vkmmldtmkn2vspkb,
            },
          },
...
```

## Limitations

Hard links are not supported at this time, but may be added in the future.
Mounting a dataset in multiple locations is not supported, but may be added in the future.

## Troubleshooting

`Mount failed: no FUSE devices found`
Make sure FUSE is installed. If you're on Mac OS X make sure the kernel module is loaded by executing `/Library/Filesystems/osxfusefs.fs/Support/load_osxfusefs`.

## Contributing

Issues welcome; testing welcome; code welcome. Feel free to pitch in!

