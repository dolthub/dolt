# Noms type language parser

The parser for the Noms type language is currently generated using
Pigeon (https://github.com/PuerkitoBio/pigeon) and goimports. To get the
packages needed to work on the parser, run

```
go get -u github.com/PuerkitoBio/pigeon github.com/bradfitz/goimports
```

Once these are set up, simply run `go generate` to generate go code for the parser.
