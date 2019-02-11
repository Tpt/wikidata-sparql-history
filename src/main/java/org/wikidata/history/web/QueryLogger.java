package org.wikidata.history.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class QueryLogger implements AutoCloseable {
  private final static Logger LOGGER = LoggerFactory.getLogger(QueryLogger.class);
  private final static int MAX_CACHE_SIZE = 128;

  private final BufferedWriter writer;
  private final List<String> buffer = new ArrayList<>();

  QueryLogger(Path logFile) throws IOException {
    writer = Files.newBufferedWriter(logFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
  }

  synchronized void logQuery(String query) {
    buffer.add(query.replaceAll("\\s+", " "));
    if (buffer.size() > MAX_CACHE_SIZE) {
      writeCache();
    }
  }

  private void writeCache() {
    Collections.shuffle(buffer);

    for (String query : buffer) {
      try {
        writer.write(query);
        writer.write('\n');
      } catch (IOException e) {
        LOGGER.error(e.getMessage(), e);
      }
    }

    buffer.clear();
  }

  @Override
  public void close() {
    writeCache();
    try {
      writer.close();
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
    }

  }
}
