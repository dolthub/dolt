# SF Crime

Import SF public crime data into noms, and search by georectangle.

## Usage

```
cd $GOPATH/src/github.com/attic-labs/noms/clients/sfcrime/importer

# Fetch the data from: http://www.opendatacache.com/data.sfgov.org/api/views/tmnf-yvry/rows.csv

go build
./importer -ldb="/tmp/sfcrime" -ds="sfcrime/raw" -input-file="rows.csv"

# quad_tree is a general purpose tool to index geocoded data
cd ../../quad_tree
go build
./quad_tree -ldb="/tmp/sfcrime" -input-ref="<ref-from-importer-output>" -output-ds="sfcrime/bygeo"

cd ../sfcrime/search
go build
./search -ldb="/tmp/sfcrime" -quadtree-ref="<ref-from-quadtree-output>" -lat="37.7806521" -lon="-122.4070723"
```
