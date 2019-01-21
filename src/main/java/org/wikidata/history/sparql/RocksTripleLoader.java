package org.wikidata.history.sparql;

import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

public final class RocksTripleLoader implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(RocksTripleLoader.class);

  private final RocksStore store;
  private final NumericValueFactory valueFactory;

  public RocksTripleLoader(Path path) {
    store = new RocksStore(path, false);
    valueFactory = new NumericValueFactory(store.getReadWriteStringStore());
  }

  public void load(Path file) throws IOException {
    RocksStore.Index<long[], long[]> spoIndex = store.spoStatementIndex();
    RocksStore.Index<long[], long[]> posIndex = store.posStatementIndex();
    RocksStore.Index<long[], long[]> ospIndex = store.ospStatementIndex();

    LOGGER.info("Loading triples");
    loadTriples(file, spoIndex, posIndex, ospIndex);

    try {
      LOGGER.info("Computing P279 closure");
      computeClosure(
              valueFactory.encodeValue(valueFactory.createIRI(Vocabulary.WDT_NAMESPACE, "P279")),
              valueFactory.encodeValue(Vocabulary.P279_CLOSURE),
              spoIndex,
              posIndex,
              ospIndex);
    } catch (NotSupportedValueException e) {
      // Should never happen
    }
  }

  private BufferedReader gzipReader(Path path) throws IOException {
    return new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(path))));
  }

  private void loadTriples(Path path, RocksStore.Index<long[], long[]> spoIndex, RocksStore.Index<long[], long[]> posIndex, RocksStore.Index<long[], long[]> ospIndex) throws IOException {
    AtomicLong done = new AtomicLong();
    try (BufferedReader reader = gzipReader(path)) {
      reader.lines().parallel().peek(line -> {
        long count = done.getAndIncrement();
        if (count % 1_000_000 == 0) {
          LOGGER.info(count + " triples imported");
        }
      }).forEach(line -> {
        String[] parts = line.split("\t");
        try {
          long[] revisionIds = Arrays.stream(parts[3].split(" ")).mapToLong(Long::parseLong).toArray();
          if (!LongRangeUtils.isSorted(revisionIds)) {
            LOGGER.error("the revision ranges are not sorted: " + Arrays.toString(revisionIds));
          }
          addTriple(spoIndex, posIndex, ospIndex,
                  valueFactory.encodeValue(NTriplesUtil.parseResource(parts[0], valueFactory)),
                  valueFactory.encodeValue(NTriplesUtil.parseURI(parts[1], valueFactory)),
                  valueFactory.encodeValue(NTriplesUtil.parseValue(parts[2], valueFactory)),
                  revisionIds
          );
        } catch (NotSupportedValueException e) {
          // We ignore it for now
        } catch (Exception e) {
          LOGGER.error(e.getMessage(), e);
        }
      });
    }

    LOGGER.info("Compacting store");
    store.compact();
  }

  private void computeClosure(long property, long targetProperty, RocksStore.Index<long[], long[]> spoIndex, RocksStore.Index<long[], long[]> posIndex, RocksStore.Index<long[], long[]> ospIndex) {
    //We copy everything into the closure
    posIndex.longPrefixIterator(new long[]{property})
            .forEachRemaining(entry -> addTriple(spoIndex, posIndex, ospIndex,
                    entry.getKey()[2],
                    targetProperty,
                    entry.getKey()[1],
                    entry.getValue()));

    //We compute the closure
    posIndex.longPrefixIterator(new long[]{targetProperty}).forEachRemaining(targetEntry -> {
      long[] targetTriple = targetEntry.getKey();
      long[] targetRange = targetEntry.getValue();
      posIndex.longPrefixIterator(new long[]{targetProperty, targetTriple[2]}).forEachRemaining(leftEntry -> {
        long[] range = LongRangeUtils.intersection(targetRange, leftEntry.getValue());
        if (range != null) {
          addTriple(spoIndex, posIndex, ospIndex,
                  leftEntry.getKey()[2],
                  targetProperty,
                  targetTriple[1],
                  range
          );
        }
      });
      spoIndex.longPrefixIterator(new long[]{targetTriple[1], targetProperty}).forEachRemaining(rightEntry -> {
        long[] range = LongRangeUtils.intersection(targetRange, rightEntry.getValue());
        if (range != null) {
          addTriple(spoIndex, posIndex, ospIndex,
                  targetTriple[2],
                  targetProperty,
                  rightEntry.getKey()[2],
                  range
          );
        }
      });
    });
  }

  private static void addTriple(RocksStore.Index<long[], long[]> spoIndex, RocksStore.Index<long[], long[]> posIndex, RocksStore.Index<long[], long[]> ospIndex, long subject, long predicate, long object, long[] range) {
    if (range == null) {
      throw new IllegalArgumentException("Triple without revision range");
    }
    long[] spoTriple = new long[]{subject, predicate, object};
    long[] posTriple = new long[]{predicate, object, subject};
    long[] ospTriple = new long[]{object, subject, predicate};

    long[] existingRange = spoIndex.get(spoTriple);
    if (existingRange != null) {
      range = LongRangeUtils.union(existingRange, range);
    }
    spoIndex.put(spoTriple, range);
    posIndex.put(posTriple, range);
    ospIndex.put(ospTriple, range);
  }

  @Override
  public void close() {
    valueFactory.close();
    store.close();
  }
}
