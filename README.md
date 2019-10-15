SPARQL endpoint for Wikidata history
====================================

This repository provides a SPARQL endpoint for Wikidata history, allowing to do queries like "count the number of humans in Wikidata in 2015" or "how many contributors have added values for the sex or gender property".

A [documentation page is available on Wikidata.org](https://www.wikidata.org/wiki/Wikidata:History_Query_Service).

[![Build Status](https://travis-ci.org/Tpt/wikidata-sparql-history.svg?branch=master)](https://travis-ci.org/Tpt/wikidata-sparql-history)

## Developer documentation

To setup a working endpoint do:

* Compile the Java program `mvn package`
* Download the Wikidata history dumps to a directory `mkdir dumps && cd dumps && bash ../download_wd_history.sh`. Warning: it requires around 600GB of disk.
* Preprocess the dump to get all revision metadata and triples annotated with their insertions and deletions (takes a few days and all your CPU cores): `java -server -jar target/sparql-endpoint-0.1-SNAPSHOT.jar -preprocess`
* Build database indexes: `java -server -jar target/sparql-endpoint-0.1-SNAPSHOT.jar -load`. This task is mostly I/O bounded. A (big) fast SSD helps a lot.
* Start the web server `java -server -classpath target/sparql-endpoint-0.1-SNAPSHOT.jar org.wikidata.history.web.Main`

## License

Copyright (C) 2019 Thomas Pellissier Tanon.

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

## Citation

If you want to cite this software in an academic publication, please use:

Pellissier Tanon T., Suchanek F. (2019) Querying the Edit History of Wikidata. In: Hitzler P. et al. (eds) The Semantic Web: ESWC 2019 Satellite Events. ESWC 2019. Lecture Notes in Computer Science, vol 11762. Springer, Cham

In BibTex:
```bibtex
@inproceedings{wikdiataHistoryQueryService,
  author    = {Thomas Pellissier Tanon and Fabian M. Suchanek},
  title     = {Querying the Edit History of Wikidata},
  booktitle = {The Semantic Web: {ESWC} 2019 Satellite Events - {ESWC} 2019 Satellite
               Events, Portoro{\v{z}}, Slovenia, June 2-6, 2019, Revised Selected
               Papers},
  pages     = {161--166},
  year      = {2019},
  doi       = {10.1007/978-3-030-32327-1\_32}
}
```
