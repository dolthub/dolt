
# Getting Started on Mac OS X

1. Install [FUSE for OS X](https://osxfuse.github.io/)
2. Load FUSE for OS X:
```
> sudo /Library/Filesystems/osxfusefs.fs/Support/load_osxfusefs
```
3. Create a local database:
```
> noms serve ldb:/tmp/nomsfs &
```
4. Build and run `nomsfs`:
```
> go run nomsfs.go http://localhost:8000::nomsfs directoryForMount
```

# Exploring `nomsfs`

1. Once you have a mount point and `nomfs` is running you can add/delete/rename files and directories using Finder or the command line as you would with any other file system.
2. Stop `nomfs` with `Ctrl+C`
3. Let's look around the dataset:
```
> noms ds http://localhost:8000
nomsfs
> noms show http://localhost:8000::nomsfs
struct Commit {
  meta: struct {},
  parents: Set<Ref<Cycle<0>>>,
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

