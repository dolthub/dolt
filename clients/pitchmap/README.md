# pitchmap

This directory contains a pipeline of tools that combine to generate a heatmap of locations for major league baseball pitchers.

The source data comes from:

http://gd2.mlb.com/components/game/mlb/

To use:

```
cd /tmp/foo

wget -e robots=off -A "[0-9]*.xml" -r -l1 \
http://gd2.mlb.com/components/game/mlb/year_2015/month_05/day_12/gid_2015_05_12_atlmlb_cinmlb_1/pitchers/

wget -e robots=off -A "inning_[0-9]*.xml" -r -l1 \
http://gd2.mlb.com/components/game/mlb/year_2015/month_05/day_12/gid_2015_05_12_atlmlb_cinmlb_1/inning/

<noms>/clients/xml_importer/xml_importer --file-store=/tmp/mlb_data --dataset-id=mlb/xml  gd2.mlb.com/

<noms>/clients/pitchmap/index/index --file-store=/tmp/mlb_data --input-dataset-id=mlb/xml --output-dataset-id=mlb/heatmap
```
