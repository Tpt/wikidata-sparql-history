package org.wikidata.history;

import org.apache.commons.cli.*;
import org.eclipse.rdf4j.query.resultio.text.tsv.SPARQLResultsTSVWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.history.preprocessor.HistoryOutput;
import org.wikidata.history.preprocessor.RevisionFileConverter;
import org.wikidata.history.sparql.HistoryRepository;
import org.wikidata.history.sparql.RocksRevisionLoader;
import org.wikidata.history.sparql.RocksTripleLoader;

import java.io.BufferedWriter;
import java.io.IOException;
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
    options.addOption("p", "preprocess", false, "Preprocess the data from Wikidata history XML dump compressed with bz2");
    options.addOption("l", "load", false, "Build database indexes from the preprocessed data");
    options.addOption("q", "sparql", true, "SPARQL query to execute");

    options.addOption("dd", "dumps-dir", true, "Directory to preprocess data from.");
    options.addOption("pd", "preprocessed-dir", true, "Directory where preprocessed data are.");
    options.addOption("id", "index-dir", true, "Directory where index data are.");
    options.addOption("t", "triples-only", false, "Load only triples");

    CommandLineParser parser = new DefaultParser();
    CommandLine line = parser.parse(options, args);

    Path dumpsDir = Paths.get(line.getOptionValue("dumps-dir", "dumps"));
    Path preprocessedDir = Paths.get(line.getOptionValue("preprocessed-dir", "wd-preprocessed"));
    Path indexDir = Paths.get(line.getOptionValue("index-dir", "wd-history-index"));

    if (line.hasOption("preprocess")) {
      System.setProperty("jdk.xml.entityExpansionLimit", String.valueOf(Integer.MAX_VALUE));
      System.setProperty("jdk.xml.totalEntitySizeLimit", String.valueOf(Integer.MAX_VALUE));
      if (!Files.isDirectory(preprocessedDir)) {
        Files.createDirectories(preprocessedDir);
      }

      ExecutorService executorService = Executors.newFixedThreadPool(
              Runtime.getRuntime().availableProcessors()
      );
      try (
              HistoryOutput historyOutput = new HistoryOutput(preprocessedDir);
              BufferedWriter log = Files.newBufferedWriter(preprocessedDir.resolve("logs.txt"))
      ) {
        RevisionFileConverter revisionFileConverter = new RevisionFileConverter(historyOutput);
        executorService.invokeAll(
                Files.walk(dumpsDir)
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
      if (!Files.isDirectory(indexDir)) {
        Files.createDirectories(indexDir);
      }

      if (!line.hasOption("triples-only")) {
        Path revisionsFile = preprocessedDir.resolve("revisions.tsv.gz");
        if (Files.exists(revisionsFile)) {
          try (RocksRevisionLoader loader = new RocksRevisionLoader(indexDir)) {
            loader.load(revisionsFile);
          }
        } else {
          LOGGER.warn("Skipping revisions loading " + revisionsFile + " does not exists");
        }
      }

      Path triplesFile = preprocessedDir.resolve("triples.tsv.gz");
      if (Files.exists(triplesFile)) {
        try (RocksTripleLoader loader = new RocksTripleLoader(indexDir)) {
          loader.load(triplesFile);
        }
      } else {
        LOGGER.warn("Skipping revisions loading " + triplesFile + " does not exists");
      }
    }

    if (line.hasOption("sparql")) {
      try (HistoryRepository historyRepository = new HistoryRepository(indexDir)) {
        historyRepository.getConnection().prepareTupleQuery(line.getOptionValue("sparql"))
                .evaluate((new SPARQLResultsTSVWriterFactory()).getWriter(System.out));
      }
    }
  }
}
