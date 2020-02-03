package org.wikidata.history.sparql;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
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
  private static final IRI SCHEMA_DESCRIPTION = SimpleValueFactory.getInstance().createIRI("http://schema.org/description");

  private final RocksStore store;
  private final Path countFile;
  private final boolean wdtOnly;
  private final NumericValueFactory valueFactory;
  private final RocksStore.Index<long[], long[]> spoIndex;
  private final RocksStore.Index<long[], long[]> posIndex;
  private final RocksStore.Index<long[], long[]> ospIndex;
  private final RocksStore.Index<Long, long[]> insertedStatement;
  private final RocksStore.Index<Long, long[]> deletedStatement;

  public RocksTripleLoader(Path path, boolean wdtOnly) {
    store = new RocksStore(path, false);
    countFile = path.resolve("triple-progress.txt");
    valueFactory = new NumericValueFactory(store.getReadWriteStringStore());
    spoIndex = store.spoStatementIndex();
    posIndex = store.posStatementIndex();
    ospIndex = store.ospStatementIndex();
    insertedStatement = store.insertedStatementIndex();
    deletedStatement = store.deletedStatementIndex();
    this.wdtOnly = wdtOnly;
  }

  public void load(Path file) throws IOException {
    LOGGER.info("Loading triples");
    if (wdtOnly) {
      LOGGER.info("Loading only direct properties");
    }
    loadTriples(file);

    LOGGER.info("Compacting store");
    store.compact();
  }

  private BufferedReader gzipReader(Path path) throws IOException {
    return new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(path))));
  }

  private void loadTriples(Path path) throws IOException {
    long start = 0;
    try {
      start = Long.parseLong(new String(Files.readAllBytes(countFile)).trim());
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
    }

    AtomicLong done = new AtomicLong(start);
    try (BufferedReader reader = gzipReader(path)) {
      // We skip the lines we have to skip
      for (long i = 0; i < start; i++) {
        reader.readLine();
      }

      reader.lines().parallel().peek(line -> {
        long count = done.getAndIncrement();
        if (count % 1_000_000 == 0) {
          try {
            Files.write(countFile, Long.toString(count).getBytes());
          } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
          }
          LOGGER.info(count + " triples imported");
        }
      }).forEach(line -> {
        String[] parts = line.split("\t");
        try {
          long[] revisionIds = Arrays.stream(parts[3].split(" ")).mapToLong(Long::parseLong).toArray();
          if (!LongRangeUtils.isSorted(revisionIds)) {
            LOGGER.error("the revision ranges are not sorted: " + Arrays.toString(revisionIds));
          }
          Resource subject = NTriplesUtil.parseResource(parts[0], valueFactory);
          IRI predicate = NTriplesUtil.parseURI(parts[1], valueFactory);
          Value object = NTriplesUtil.parseValue(parts[2], valueFactory);
          if (wdtOnly && !(OWL.SAMEAS.equals(predicate) || RDFS.LABEL.equals(predicate) || SCHEMA_DESCRIPTION.equals(predicate) || Vocabulary.WDT_NAMESPACE.equals(predicate.getNamespace()))) {
            return;
          }
          addTriple(
                  valueFactory.encodeValue(subject),
                  valueFactory.encodeValue(predicate),
                  valueFactory.encodeValue(object),
                  revisionIds
          );
        } catch (NotSupportedValueException e) {
          // We ignore it for now
        } catch (Exception e) {
          LOGGER.error(e.getMessage(), e);
        }
      });
    }
  }

  private void addTriple(long subject, long predicate, long object, long[] range) {
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

    // Range additions
    for (int i = 0; i < range.length; i += 2) {
      addToStatementListIndex(insertedStatement, range[i], spoTriple);
      if (range[i + 1] != Long.MAX_VALUE) {
        addToStatementListIndex(deletedStatement, range[i + 1], spoTriple);
      }
    }

    // Range deletions
    if (existingRange != null) {
      for (int i = 0; i < existingRange.length; i += 2) {
        if (!LongRangeUtils.isRangeStart(existingRange[i], range)) {
          removeFromStatementListIndex(insertedStatement, existingRange[i], spoTriple);
        }
        if (!LongRangeUtils.isRangeEnd(existingRange[i + 1], range) && existingRange[i + 1] != Long.MAX_VALUE) {
          removeFromStatementListIndex(deletedStatement, existingRange[i + 1], spoTriple);
        }
      }
    }
  }

  private static void addToStatementListIndex(RocksStore.Index<Long, long[]> index, long key, long[] triple) {
    long[] existingTriples = index.get(key);
    long[] newTriples = (existingTriples == null) ? triple : TripleArrayUtils.addToSortedArray(existingTriples, triple);
    if (newTriples != existingTriples) {
      index.put(key, newTriples);
    }
  }

  private static void removeFromStatementListIndex(RocksStore.Index<Long, long[]> index, long key, long[] triple) {
    long[] existingTriples = index.get(key);
    if (existingTriples == null) {
      return;
    }
    long[] newTriples = TripleArrayUtils.removeFromSortedArray(existingTriples, triple);
    if (newTriples != existingTriples) {
      index.put(key, newTriples);
    }
  }

  @Override
  public void close() {
    valueFactory.close();
    store.close();
  }
}
