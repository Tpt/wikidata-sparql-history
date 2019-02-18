package org.wikidata.history.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class QueryLogger implements AutoCloseable {
  private final static Logger LOGGER = LoggerFactory.getLogger(QueryLogger.class);
  private final static int MAX_CACHE_SIZE = 128;

  private final Path logDirectory;
  private final List<String> buffer = new ArrayList<>();
  private LocalDate bufferLocalDate = LocalDate.now();

  QueryLogger(Path logDirectory) throws IOException {
    if (!Files.exists(logDirectory)) {
      Files.createDirectory(logDirectory);
    }
    if (!Files.isDirectory(logDirectory)) {
      throw new IOException("The element " + logDirectory + " is not a directory.");
    }
    this.logDirectory = logDirectory;
  }

  synchronized void logQuery(String query) {
    LocalDate date = LocalDate.now();
    if (!bufferLocalDate.equals(date)) {
      writeCache();
      bufferLocalDate = date;
    }
    buffer.add(query.replaceAll("\\s+", " "));
    if (buffer.size() > MAX_CACHE_SIZE) {
      writeCache();
    }
  }

  private void writeCache() {
    if (buffer.isEmpty()) {
      return;
    }

    Collections.shuffle(buffer);

    Path logFile = logDirectory.resolve(bufferLocalDate.toString() + ".txt");
    try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
      for (String query : buffer) {
        writer.write(query);
        writer.write('\n');
      }
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
    }

    buffer.clear();
  }

  @Override
  public void close() {
    writeCache();
  }
}
