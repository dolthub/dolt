# CSV Importer

Imports a CSV file as List<T> where T is generated from the header row of the CSV (this can also be overridden with the `-header` flag).

## Usage

To import a CSV file do:

```
$ go build csv_importer.go
$ ./csv_importer -fs="/tmp/foo" -ds="foo" <URL>
```

## List of CSV URLs
 - https://data.cityofnewyork.us/api/views/kku6-nxdu/rows.csv?accessType=DOWNLOAD
