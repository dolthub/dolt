# multicodec tool

This is the multicodec tool. It is useful to inspect data which has been encoded with many different multicodec codecs.

## Usage

```sh
> ./multicodec -h
multicodec - tool to inspect and manipulate mixed codec streams

Usage
  cat rawjson | multicodec --wrap /json/msgio >mcjson
  cat rawcbor | multicodec --wrap /cbor >mccbor

  cat mixed | multicodec -r /json/msgio >all_in_json
  cat mixed | multicodec -f /json/msgio >json_ones_only

  cat mixed | multicodec --headers >all_headers
  cat mixed | multicodec --paths >all_paths

  cat paths   | multicodec --p2h >headers
  cat headers | multicodec --h2p >paths

Options
  -f, --filter    filter items of given codec
  -r, --recode    recode items to given codec
  -w, --wrap      wrap raw data with header

  --mcwrap        item headers wrapped with /multicodec
  --msgio         wrap all subcodecs with /msgio

  --headers       output only items' headers
  --paths         output only items' header paths

  --h2p           convert headers to line-delimited paths
  --p2h           convert line-delimited paths to headers
```

## Examples

[See the examples](examples) included, made with:

```
cat examples/one.json | ./multicodec -r /cbor >examples/one.cbor
cat examples/many.json | ./multicodec -r /cbor >examples/many.cbor
cat examples/one.json >examples/four.mixed
cat examples/one.cbor >>examples/four.mixed
cat examples/one.json >>examples/four.mixed
cat examples/one.cbor >>examples/four.mixed
cat examples/four.mixed | ./multicodec -r /json >examples/four.unmixed.json
cat examples/four.mixed | ./multicodec -r /cbor >examples/four.unmixed.cbor
cat examples/four.mixed | ./multicodec --headers >examples/four.headers
cat examples/four.mixed | ./multicodec --paths >examples/four.paths
```
