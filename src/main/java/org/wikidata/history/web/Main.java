/*
 * Copyright (C) 2017 Simple WD Developers
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wikidata.history.web;

import io.javalin.Javalin;
import org.apache.commons.cli.*;
import org.wikidata.history.sparql.RocksTripleSource;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Main {

  public static void main(String[] args) throws ParseException, IOException {
    Options options = new Options();
    options.addOption("i", "index", true, "Directory where the index data are.");
    options.addOption("h", "host", true, "Host name");
    options.addOption("p", "port", true, "Name of the port to listen from");
    options.addOption("l", "logFile", true, "Name of the query log file. By default query-log.txt");

    CommandLineParser parser = new DefaultParser();
    CommandLine line = parser.parse(options, args);
    Path indexPath = Paths.get(line.getOptionValue("index", "wd-history-index"));
    Path queryLog = Paths.get(line.getOptionValue("logFile", "query-log"));

    String portString = line.getOptionValue("port", System.getenv("PORT"));
    int port = (portString != null) ? Integer.parseInt(portString) : 7000;

    RocksTripleSource tripleSource = new RocksTripleSource(indexPath);
    QueryLogger queryLogger = new QueryLogger(queryLog);
    SparqlEndpoint sparqlEndpoint = new SparqlEndpoint(tripleSource, queryLogger);
    Javalin javalin = Javalin.create()
            .get("", ctx -> ctx.contentType("text/html").result(Main.class.getResourceAsStream("/index.html")))
            .get("/sparql", sparqlEndpoint::get)
            .post("/sparql", sparqlEndpoint::post)
            .get("/prefixes", ctx -> ctx.contentType("application/json").result(Main.class.getResourceAsStream("/prefixes.json")))
            .start(port);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      javalin.stop();
      queryLogger.close();
      tripleSource.close();
    }));
  }
}
