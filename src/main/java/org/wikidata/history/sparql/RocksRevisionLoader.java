package org.wikidata.history.sparql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

public final class RocksRevisionLoader implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(RocksRevisionLoader.class);
  private static final long[] EMPTY_ARRAY = new long[]{};

  private final NumericValueFactory valueFactory;
  private final RocksStore store;

  public RocksRevisionLoader(Path path) {
    LOGGER.info("Loading revision data to " + path);

    valueFactory = new NumericValueFactory(new NumericValueFactory.EmptyStringStore());
    store = new RocksStore(path, false);
  }

  public void load(Path file) throws IOException {
    RocksStore.Index<Long, Long> revisionDateOutput = store.revisionDateIndex();
    RocksStore.Index<Long, long[]> dateRevisionsOutput = store.dateRevisionsIndex();
    RocksStore.Index<Long, Long> parentRevisionOutput = store.parentRevisionIndex();
    RocksStore.Index<Long, Long> childRevisionOutput = store.childRevisionIndex();
    RocksStore.Index<Long, Long> revisionTopicOutput = store.revisionTopicIndex();
    RocksStore.Index<Long, long[]> topicRevisionsOutput = store.topicRevisionIndex();
    RocksStore.Index<Long, String> revisionContributorOutput = store.revisionContributorIndex();

    try (BufferedReader reader = gzipReader(file)) {
      reader.lines().parallel().forEach(line -> {
        String[] parts = line.split("\t");
        long revisionId = Long.parseLong(parts[0]);
        long parentRevisionId = Long.parseLong(parts[1]);
        long timestamp = Long.parseLong(parts[3]);
        String contributor = parts[4];

        if (parentRevisionId >= 0) {
          parentRevisionOutput.put(revisionId, parentRevisionId);
          childRevisionOutput.put(parentRevisionId, revisionId);
        }

        try {
          long entity = valueFactory.encodeValue(valueFactory.createIRI(Vocabulary.WD_NAMESPACE, parts[2]));
          revisionTopicOutput.put(revisionId, entity);
          addToMultipleValuesIndex(topicRevisionsOutput, entity, revisionId);
        } catch (NotSupportedValueException e) {
          LOGGER.error(e.getMessage(), e);
        }

        revisionDateOutput.put(revisionId, timestamp);
        addToMultipleValuesIndex(dateRevisionsOutput, timestamp, revisionId);

        revisionContributorOutput.put(revisionId, contributor);
      });
    }

    System.out.println("Compacting store");
    store.compact();
  }

  private BufferedReader gzipReader(Path path) throws IOException {
    return new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(path))));
  }

  private <K> void addToMultipleValuesIndex(RocksStore.Index<K, long[]> index, K key, long value) {
    long[] otherValues = index.getOrDefault(key, EMPTY_ARRAY);
    long[] allValues = Arrays.copyOfRange(otherValues, 0, otherValues.length + 1);
    allValues[otherValues.length] = value;
    index.put(key, allValues);
  }

  @Override
  public void close() {
    valueFactory.close();
    store.close();
  }
}
