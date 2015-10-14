Slurps mp3 files into a noms db.

Depends on [id3-go](https://github.com/mikkyang/id3-go). To install:
```
  cd "$GOPATH/src/github.com"
  git clone https://github.com/mikkyang/id3-go.git
```

Possible usage, if you have mp3 files in your Music directory:
```
  find ~/Music -name '*.mp3' -exec ./mp3_importer -ldb /tmp/mp3_importer -ds main -add {} \;
  ./mp3_importer -ldb /tmp/mp3_importer -ds main -list
```
