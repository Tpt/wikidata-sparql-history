SPARQL endpoint for Wikidata history
====================================

This repository provides a SPARQL endpoint for Wikidata history, allowing to do queries like "count the number of humans in Wikidata in 2015" or "how many contributors have added values for the sex or gender property".

Warning: This is a work in progress and is not ready yet.

[![Build Status](https://travis-ci.org/Tpt/wikidata-sparql-history.svg?branch=master)](https://travis-ci.org/Tpt/wikidata-sparql-history)


## User documentation

A public endpoint should be available soon. Here are some example of queries:


## Developer documentation

To setup a working endpoint do:

* Compile the Java program `mvn package`
* Download the Wikidata history dumps to a directory `mkdir dumps && cd dumps && bash ../download_wd_history.sh`. Warning: it requies around 600GB of disk.
* Preprocess the dump to get all revision metadata and triples annotated with there insertions and deletions (takes a few days and all your CPU cores): `java -server -jar target/sparql-endpoint-0.1-SNAPSHOT.jar -preprocess`
* Build database indexes: `java -server -jar target/sparql-endpoint-0.1-SNAPSHOT.jar -load`. You may use the `--wdt-only` argument to only load wdt: triples
* Start the web server `java -server -classpath target/sparql-endpoint-0.1-SNAPSHOT.jar org.wikidata.history.web.Main`

## License

Copyright (C) 2019 Thomas Pellissier Tanon.

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.