# CSV Importer

Imports a CSV file as `List<T>` where `T` is a struct with fields corresponding to the CSV's column headers. The struct spec can also be set manually with the `-header` flag.

## Usage

```
$ cd csv-import
$ go build
$ ./csv-import <PATH> http://localhost:8000::foo
```

## Some places for CSV files

- https://data.cityofnewyork.us/api/views/kku6-nxdu/rows.csv?accessType=DOWNLOAD
- http://www.opendatacache.com/

# CSV Exporter

Export a dataset in CSV format to stdout with column headers.

## Usage

```
$ cd csv-export
$ go build
$ ./csv-export http://localhost:8000:foo
```
