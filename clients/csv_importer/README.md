# CSV Importer

Imports a CSV file as a List of Maps where the first row of the CSV file
describes the keys of the Map.

## Usage

To import a CSV file do:

```
$ go build csv_importer.go
$ ./csv_importer -fs="/tmp/foo" -ds="foo" <URL>
```

## List of CSV URLs
 - https://data.cityofnewyork.us/api/views/kku6-nxdu/rows.csv?accessType=DOWNLOAD
