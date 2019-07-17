#!/usr/bin/env bash

curl https://dumps.wikimedia.org/wikidatawiki/latest/ | grep -Po "wikidatawiki-latest-pages-meta-history[0-9]+\.xml-[p0-9]+\.bz2" | while read -r url ; do
echo $url
wget -c "https://dumps.wikimedia.org/wikidata/latest/$url"
done
