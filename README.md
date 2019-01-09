SPARQL endpoint for Wikidata history
====================================

This repository provides a SPARQL endpoint for Wikidata history, allowing to do queries like "count the number of humans in Wikidata in 2015" or "how many contributors have added values for the sex or gender property".

Warning: This is a work in progress and is not ready yet.

[![Build Status](https://travis-ci.org/Tpt/wikidata-sparql-history.svg?branch=master)](https://travis-ci.org/Tpt/wikidata-sparql-history)


## User documentation

A public endpoint should be available soon. Here are some example of queries:

Number of humans in Wikidata in February 2nd, 2015 at midnight.
```sparql
SELECT (COUNT(?item) AS ?count) WHERE {
  ?rev schema:dateCreated "2015-02-02T00:00:00Z"^^xsd:dateTime ; 
       hist:globalState ?state .
  GRAPH ?state {
    ?item wdt:P31 wd:Q5
  }
}
```

Number of contributors having changed sex or gender values:
```sparql
SELECT (COUNT(?user) AS ?count) WHERE {
  # this is going to only set in ?addOrDel the graphs where a value of wd:P21 is added or removed
  GRAPH ?addOrDel {
    ?item wdt:P21 ?value .
  }
  ?rev hist:additions|hist:deletions ?addOrDel ;
       schema:author ?user .
}
```

These queries assumes the following prefixes:
```sparql
PREFIX schema: <http://schema.org/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX hist: <http://wikiba.se/history/ontology#>
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
```


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