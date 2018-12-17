package org.wikidata.history;

import org.apache.commons.cli.*;
import org.eclipse.rdf4j.query.resultio.text.tsv.SPARQLResultsTSVWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.history.preprocessor.HistoryOutput;
import org.wikidata.history.preprocessor.RevisionFileConverter;
import org.wikidata.history.sparql.HistoryRepository;
import org.wikidata.history.sparql.MapDBRevisionLoader;
import org.wikidata.history.sparql.MapDBTripleLoader;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class Main {
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws ParseException, IOException, InterruptedException {
    Options options = new Options();
    options.addOption("p", "preprocess", true, "Directory to preprocess data from. Useful to build the files that should be then loaded to build the indexes");
    options.addOption("l", "load", true, "Directory to load data from. Useful to build the indexes");
    options.addOption("t", "triplesOnly", false, "Load only triples");
    options.addOption("q", "sparql", true, "SPARQL query to execute");

    CommandLineParser parser = new DefaultParser();
    CommandLine line = parser.parse(options, args);

    Path index = Paths.get("wd-history-index");
    if (line.hasOption("preprocess")) {
      System.setProperty("jdk.xml.entityExpansionLimit", String.valueOf(Integer.MAX_VALUE));
      System.setProperty("jdk.xml.totalEntitySizeLimit", String.valueOf(Integer.MAX_VALUE));


      ExecutorService executorService = Executors.newFixedThreadPool(
              Runtime.getRuntime().availableProcessors()
      );
      try (
              HistoryOutput historyOutput = new HistoryOutput(Paths.get("out"));
              BufferedWriter log = Files.newBufferedWriter(Paths.get("out/logs.txt"))
      ) {
        RevisionFileConverter revisionFileConverter = new RevisionFileConverter(historyOutput);
        executorService.invokeAll(
                Files.walk(Paths.get("."))
                        .filter(file -> file.toString().endsWith(".bz2"))
                        .map(file -> (Callable<Void>) () -> {
                          try {
                            revisionFileConverter.process(file);
                            log.write(file.toString() + "\tok\n");
                          } catch (Exception e) {
                            LOGGER.error(e.getMessage(), e);
                            log.write(file.toString() + "\terror\t" + e.getMessage() + "\n");
                          }
                          return null;
                        })
                        .collect(Collectors.toList())
        );
      } finally {
        executorService.shutdown();
      }
    }

    if (line.hasOption("load")) {
      try {
        Files.createDirectories(index);
      } catch (FileAlreadyExistsException e) {
        //Don't care
      }
      if (!line.hasOption("triplesOnly")) {
        try (MapDBRevisionLoader loader = new MapDBRevisionLoader(index)) {
          loader.load(Paths.get(line.getOptionValue("load")));
        }
      }
      try (MapDBTripleLoader loader = new MapDBTripleLoader(index)) {
        loader.load(Paths.get(line.getOptionValue("load")));
      }
    }

    if (line.hasOption("sparql")) {
      try (HistoryRepository historyRepository = new HistoryRepository(index)) {
        historyRepository.getConnection().prepareTupleQuery(line.getOptionValue("sparql"))
                .evaluate((new SPARQLResultsTSVWriterFactory()).getWriter(System.out));
      }
    }
  }
}
