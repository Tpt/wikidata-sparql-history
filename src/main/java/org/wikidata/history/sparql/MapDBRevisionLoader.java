package org.wikidata.history.sparql;

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
import java.util.zip.GZIPInputStream;

public final class MapDBRevisionLoader implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MapDBRevisionLoader.class);
  private static final long[] EMPTY_ARRAY = new long[]{};

  private final NumericValueFactory valueFactory;
  private final MapDBStore store;

  public MapDBRevisionLoader(Path path) {
    LOGGER.info("Loading revision data to " + path);

    valueFactory = new NumericValueFactory(new NumericValueFactory.EmptyStringStore());
    store = new MapDBStore(path);
  }

  public void load(Path directory) throws IOException {
    loadRevisions(directory.resolve("revisions.tsv.gz"));
  }

  private BufferedReader gzipReader(Path path) throws IOException {
    return new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(path))));
  }

  private void loadRevisions(Path path) throws IOException {
    try (DB db = DBMaker.memoryDB().make()) {
      BTreeMap<Long, Long> tempRevisionDateOutput = newMemoryTreeMap(db, "revision_date", Serializer.LONG, Serializer.LONG);
      BTreeMap<Long, Long> tempParentRevisionOutput = newMemoryTreeMap(db, "parent_revision", Serializer.LONG, Serializer.LONG);
      BTreeMap<Long, Long> tempChildRevisionOutput = newMemoryTreeMap(db, "child_revision", Serializer.LONG, Serializer.LONG);
      BTreeMap<Long, Long> tempRevisionTopicOutput = newMemoryTreeMap(db, "revision_topic", Serializer.LONG, Serializer.LONG);
      BTreeMap<Long, long[]> tempTopicRevisionsOutput = newMemoryTreeMap(db, "topic_revision", Serializer.LONG, Serializer.LONG_ARRAY);
      BTreeMap<Long, String> tempRevisionContributorOutput = newMemoryTreeMap(db, "revision_contributor", Serializer.LONG, Serializer.STRING);

      try (BufferedReader reader = gzipReader(path)) {
        reader.lines().forEach(line -> {
          String[] parts = line.split("\t");
          long revisionId = Long.parseLong(parts[0]);
          long parentRevisionId = Long.parseLong(parts[1]);
          long timestamp = Long.parseLong(parts[3]);
          String contributor = parts[4];

          if (parentRevisionId >= 0) {
            tempParentRevisionOutput.put(revisionId, parentRevisionId);
            tempChildRevisionOutput.put(parentRevisionId, revisionId);
          }

          try {
            long entity = valueFactory.encodeValue(valueFactory.createIRI(Vocabulary.WD_NAMESPACE, parts[2]));
            long[] otherRevisionIds = tempTopicRevisionsOutput.getOrDefault(entity, EMPTY_ARRAY);
            long[] allRevisionIds = Arrays.copyOfRange(otherRevisionIds, 0, otherRevisionIds.length + 1);
            allRevisionIds[otherRevisionIds.length] = revisionId;
            tempRevisionTopicOutput.put(revisionId, entity);
            tempTopicRevisionsOutput.put(entity, allRevisionIds);
          } catch (NotSupportedValueException e) {
          } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
          }

          tempRevisionDateOutput.put(revisionId, timestamp);
          tempRevisionContributorOutput.put(revisionId, contributor);
        });
      }

      LOGGER.info("Saving revision indexes");
      store.revisionDateIndex().buildFrom(tempChildRevisionOutput);
      store.parentRevisionIndex().buildFrom(tempParentRevisionOutput);
      store.childRevisionIndex().buildFrom(tempChildRevisionOutput);
      store.revisionTopicIndex().buildFrom(tempRevisionTopicOutput);
      store.topicRevisionIndex().buildFrom(tempTopicRevisionsOutput);
      store.revisionContributorIndex().buildFrom(tempRevisionContributorOutput);
      LOGGER.info("Revision indexes saving done");
    }
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
