# pitchmap

This directory contains a pipeline of tools that combine to generate a heatmap of locations for major league baseball pitchers.

The source data comes from:

http://gd2.mlb.com/components/game/mlb/

To use:

```

npm install

mkdir /tmp/mlb_data

node fetch-urls.js --url="http://gd2.mlb.com/components/game/mlb/year_2015/month_05/day_07/" --print="gid.*pitcher.*xml$|gid.*inning_[0-9]*\.xml" --reject="\/year_[0-9]+\/pitchers\/|\/year_[0-9]+\/mobile\/|\/year_[0-9]+\/media\/|\/year_[0-9]+\/batters\/|\/premium\/|\/notifications\/|\/pitching_staff\/|\/media\/|\/batters\/|\/[^\/]+\.[^\/]+$" > /tmp/mlb_data/urls.txt

cd /tmp/mlb_data

wget -i urls.txt

<noms>/clients/xml_importer/xml_importer --file-store=/tmp/mlb_data --dataset-id=mlb/xml  gd2.mlb.com/

<noms>/clients/pitchmap/index/index --file-store=/tmp/mlb_data --input-dataset-id=mlb/xml --output-dataset-id=mlb/heatmap
```
