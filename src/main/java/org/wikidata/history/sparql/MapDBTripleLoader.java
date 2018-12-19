package org.wikidata.history.sparql;

import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.serializer.GroupSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

public final class MapDBTripleLoader implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MapDBTripleLoader.class);

  private final MapDBStore store;
  private final MapDBStore.MapDBStringStore stringStore;
  private final NumericValueFactory valueFactory;

  public MapDBTripleLoader(Path path) {
    store = new MapDBStore(path);
    stringStore = store.newInMemoryStringStore();
    valueFactory = new NumericValueFactory(stringStore);
  }

  public void load(Path directory) throws IOException {
    try (DB db = DBMaker.memoryDB().make()) {
      BTreeMap<NumericTriple, long[]> spoIndex = newMemoryTreeMap(db, "spo", NumericTriple.SPOSerializer, Serializer.LONG_ARRAY);
      BTreeMap<NumericTriple, long[]> posIndex = newMemoryTreeMap(db, "pos", NumericTriple.POSSerializer, Serializer.LONG_ARRAY);

      LOGGER.info("Loading triples");
      loadTriples(directory.resolve("triples.tsv.gz"), spoIndex, posIndex);

      LOGGER.info("Saving string store");
      store.saveStringStore(stringStore);

      try {
        LOGGER.info("Computing P279 closure");
        computeClosure(
                valueFactory.encodeValue(valueFactory.createIRI(Vocabulary.WDT_NAMESPACE, "P279")),
                valueFactory.encodeValue(Vocabulary.P279_CLOSURE),
                spoIndex,
                posIndex);

      } catch (NotSupportedValueException e) {
        // Should never happen
      }

      LOGGER.info("Saving content triples");
      store.spoStatementIndex().buildFrom(spoIndex);
      store.posStatementIndex().buildFrom(posIndex);
      LOGGER.info("Content saving done");
    }
  }

  private BufferedReader gzipReader(Path path) throws IOException {
    return new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(path))));
  }

  private void loadTriples(Path path, BTreeMap<NumericTriple, long[]> spoIndex, BTreeMap<NumericTriple, long[]> posIndex) throws IOException {
    AtomicLong done = new AtomicLong();
    try (BufferedReader reader = gzipReader(path)) {
      reader.lines().peek(line -> {
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
          addTriple(spoIndex, posIndex,
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
  }

  private void computeClosure(long property, long targetProperty, BTreeMap<NumericTriple, long[]> spoIndex, BTreeMap<NumericTriple, long[]> posIndex) {
    //We copy everything into the closure
    entryIterator(posIndex, 0, property, 0)
            .forEachRemaining(entry -> addTriple(spoIndex, posIndex,
                    entry.getKey().getSubject(),
                    targetProperty,
                    entry.getKey().getObject(),
                    entry.getValue()));

    //We compute the closure
    entryIterator(posIndex, 0, targetProperty, 0).forEachRemaining(targetEntry -> {
      NumericTriple targetTriple = targetEntry.getKey();
      long[] targetRange = targetEntry.getValue();
      entryIterator(posIndex, 0, targetProperty, targetTriple.getSubject()).forEachRemaining(leftEntry -> {
        long[] range = LongRangeUtils.intersection(targetRange, leftEntry.getValue());
        if (range != null) {
          addTriple(spoIndex, posIndex,
                  leftEntry.getKey().getSubject(),
                  targetProperty,
                  targetTriple.getObject(),
                  range
          );
        }
      });
      entryIterator(spoIndex, targetTriple.getObject(), targetProperty, 0).forEachRemaining(rightEntry -> {
        long[] range = LongRangeUtils.intersection(targetRange, rightEntry.getValue());
        if (range != null) {
          addTriple(spoIndex, posIndex,
                  targetTriple.getSubject(),
                  targetProperty,
                  rightEntry.getKey().getObject(),
                  range
          );
        }
      });
    });
  }

  private static <K, V> Iterator<Map.Entry<K, V>> entryIterator(BTreeMap<K, V> map, K prefix) {
    return map.entryIterator(prefix, true, map.getKeySerializer().nextValue(prefix), false);
  }

  private static <V> Iterator<Map.Entry<NumericTriple, V>> entryIterator(BTreeMap<NumericTriple, V> map, long subject, long predicate, long object) {
    return entryIterator(map, new NumericTriple(subject, predicate, object));
  }

  private static void addTriple(BTreeMap<NumericTriple, long[]> spoIndex, BTreeMap<NumericTriple, long[]> posIndex, long subject, long predicate, long object, long[] range) {
    if (range == null) {
      throw new IllegalArgumentException("Triple without revision range");
    }
    NumericTriple newTriple = new NumericTriple(subject, predicate, object);
    long[] existingRange = spoIndex.get(newTriple);
    if (existingRange != null) {
      range = LongRangeUtils.union(existingRange, range);
    }
    spoIndex.put(newTriple, range);
    posIndex.put(newTriple, range);
  }

  private <K, V> BTreeMap<K, V> newMemoryTreeMap(DB db, String name, GroupSerializer<K> keySerializer, GroupSerializer<V> valueSerializer) {
    return db.treeMap(name).keySerializer(keySerializer).valueSerializer(valueSerializer).createOrOpen();
  }

  @Override
  public void close() {
    valueFactory.close();
    store.close();
  }
}
