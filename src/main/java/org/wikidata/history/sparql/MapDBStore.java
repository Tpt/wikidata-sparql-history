package org.wikidata.history.sparql;

import org.mapdb.*;
import org.mapdb.serializer.GroupSerializer;
import org.mapdb.volume.MappedFileVol;
import org.mapdb.volume.Volume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

class MapDBStore implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(MapDBStore.class);
  private Path directory;

  MapDBStore(Path directory) {
    this.directory = directory;
  }

  @Override
  public void close() {
  }

  Index<Long, Long> revisionDateIndex() {
    return new Index<>(directory.resolve("revision_date.db"), Serializer.LONG_PACKED, Serializer.LONG_PACKED);
  }

  Index<Long, Long> parentRevisionIndex() {
    return new Index<>(directory.resolve("parent_revision.db"), Serializer.LONG_PACKED, Serializer.LONG_PACKED);
  }

  Index<Long, Long> childRevisionIndex() {
    return new Index<>(directory.resolve("child_revision.db"), Serializer.LONG_PACKED, Serializer.LONG_PACKED);
  }

  Index<Long, Long> revisionTopicIndex() {
    return new Index<>(directory.resolve("revision_topic.db"), Serializer.LONG_PACKED, Serializer.LONG);
  }

  Index<Long, long[]> topicRevisionIndex() {
    return new Index<>(directory.resolve("topic_revision.db"), Serializer.LONG, Serializer.LONG_ARRAY);
  }

  Index<Long, String> revisionContributorIndex() {
    return new Index<>(directory.resolve("revision_contributor.db"), Serializer.LONG_PACKED, Serializer.STRING_DELTA);
  }

  Index<NumericTriple, long[]> spoStatementIndex() {
    return new Index<>(directory.resolve("statement_spo.db"), NumericTriple.SPOSerializer, Serializer.LONG_ARRAY);
  }

  Index<NumericTriple, long[]> posStatementIndex() {
    return new Index<>(directory.resolve("statement_pos.db"), NumericTriple.POSSerializer, Serializer.LONG_ARRAY);
  }

  private Index<String, Long> stringEncoderIndex() {
    return new Index<>(directory.resolve("string_encoder.db"), Serializer.STRING_DELTA, Serializer.LONG_PACKED);
  }

  private Index<Long, String> stringDecoderIndex() {
    return new Index<>(directory.resolve("string_decoder.db"), Serializer.LONG_PACKED, Serializer.STRING_DELTA);
  }

  private Index<String, Short> languageCodeEncoderIndex() {
    return new Index<>(directory.resolve("language_code_encoder.db"), Serializer.STRING_DELTA, Serializer.SHORT);
  }

  private Index<Short, String> languageCodeDecoderIndex() {
    return new Index<>(directory.resolve("language_code_decoder.db"), Serializer.SHORT, Serializer.STRING_DELTA);
  }

  StaticMapDBStringStore openStringStore() {
    return new StaticMapDBStringStore(
            stringEncoderIndex().newReader(),
            stringDecoderIndex().newReader(),
            languageCodeEncoderIndex().newReader(),
            languageCodeDecoderIndex().newReader()
    );
  }

  MapDBStringStore newInMemoryStringStore() {
    return new MapDBStringStore();
  }

  void saveStringStore(MapDBStringStore stringStore) {
    stringEncoderIndex().buildFrom(stringStore.encodeString);
    stringDecoderIndex().buildFrom(stringStore.decodeString);
    languageCodeEncoderIndex().buildFrom(stringStore.encodeLanguage);
    languageCodeDecoderIndex().buildFrom(stringStore.decodeLanguage);
  }

  static class Index<K, V> {

    private static final int PAGE_SHIFT = 25;

    private Path file;
    private GroupSerializer<K> keySerializer;
    private GroupSerializer<V> valueSerializer;

    private Index(Path file, GroupSerializer<K> keySerializer, GroupSerializer<V> valueSerializer) {
      this.file = file;
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
    }

    IndexBuilder<K, V> newBuilder() {
      return new IndexBuilder<>(file, keySerializer, valueSerializer);
    }

    void buildFrom(Map<? extends K, ? extends V> map) {
      try (IndexBuilder<K, V> builder = newBuilder()) {
        builder.putAll(map);
      }
    }

    IndexReader<K, V> newReader() {
      return new IndexReader<>(file, keySerializer, valueSerializer);
    }
  }

  static class IndexBuilder<K, V> implements AutoCloseable {

    private SortedTableMap.Sink<K, V> sink;

    private IndexBuilder(Path file, GroupSerializer<K> keySerializer, GroupSerializer<V> valueSerializer) {
      Volume volume = openVolume(file);
      sink = SortedTableMap.create(volume, keySerializer, valueSerializer).createFromSink();
    }

    private Volume openVolume(Path file) {
      DBException.VolumeIOError error = null;
      for (int i = 0; i < 16; i++) {
        try {
          return MappedFileVol.FACTORY.makeVolume(file.toAbsolutePath().toString(), false, 0, Index.PAGE_SHIFT, 0, false);
        } catch (DBException.VolumeIOError e) {
          error = e;
        }
        try {
          TimeUnit.MINUTES.sleep(1);
        } catch (InterruptedException e) {
          LOGGER.error(e.getMessage(), e);
        }
      }
      throw error;
    }

    void putAll(Map<? extends K, ? extends V> map) {
      map.forEach(sink::put);
    }

    @Override
    public void close() {
      sink.create().close();
    }
  }

  static class IndexReader<K, V> implements AutoCloseable, Map<K, V> {

    private SortedTableMap<K, V> map;

    private IndexReader(Path file, GroupSerializer<K> keySerializer, GroupSerializer<V> valueSerializer) {
      Volume volume = openVolume(file);
      map = SortedTableMap.open(volume, keySerializer, valueSerializer);
    }

    private Volume openVolume(Path file) {
      DBException.VolumeIOError error = null;
      for (int i = 0; i < 16; i++) {
        try {
          return MappedFileVol.FACTORY.makeVolume(file.toAbsolutePath().toString(), true, 0, Index.PAGE_SHIFT, 0, false);
        } catch (DBException.VolumeIOError e) {
          error = e;
        }
        try {
          TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
          LOGGER.error(e.getMessage(), e);
        }
      }
      throw error;
    }

    @Override
    public int size() {
      return map.size();
    }

    @Override
    public boolean isEmpty() {
      return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object o) {
      return map.containsKey(o);
    }

    @Override
    public boolean containsValue(Object o) {
      return map.containsValue(o);
    }

    @Override
    public V get(Object o) {
      return map.get(o);
    }

    @Override
    public V put(K k, V v) {
      throw new UnsupportedOperationException();
    }

    @Override
    public V remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Set<K> keySet() {
      return map.keySet();
    }

    @Override
    public Collection<V> values() {
      return map.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
      return map.entrySet();
    }

    public Iterator<Entry<K, V>> entryIterator() {
      return map.entryIterator();
    }

    public Iterator<Entry<K, V>> entryIterator(K prefix) {
      return map.entryIterator(prefix, true, map.getKeySerializer().nextValue(prefix), false);
    }

    @Override
    public void close() {
      map.close();
    }
  }

  static class MapDBStringStore implements NumericValueFactory.StringStore {

    private final DB db;
    private final BTreeMap<String, Long> encodeString;
    private final BTreeMap<Long, String> decodeString;
    private final Atomic.Long stringCounter;
    private final BTreeMap<String, Short> encodeLanguage;
    private final BTreeMap<Short, String> decodeLanguage;
    private final Atomic.Integer languageCounter;

    private MapDBStringStore() {
      db = DBMaker.memoryDB().allocateStartSize(1024 * 1024 * 1024)
              .allocateIncrement(256 * 1024 * 1024)
              .closeOnJvmShutdown()
              .make();
      encodeString = newEncodeStringMaker().createOrOpen();
      decodeString = newDecodeStringMaker().createOrOpen();
      stringCounter = newStringCounterMaker().createOrOpen();
      encodeLanguage = newEncodeLanguageMaker().createOrOpen();
      decodeLanguage = newDecodeLanguageMaker().createOrOpen();
      languageCounter = newLanguageCounterMaker().createOrOpen();
    }

    private DB.TreeMapMaker<String, Long> newEncodeStringMaker() {
      return db.treeMap("encode_string", Serializer.STRING, Serializer.LONG_PACKED);
    }

    private DB.TreeMapMaker<Long, String> newDecodeStringMaker() {
      return db.treeMap("decode_string", Serializer.LONG_PACKED, Serializer.STRING);
    }

    private DB.AtomicLongMaker newStringCounterMaker() {
      return db.atomicLong("stringCounter");
    }

    private DB.TreeMapMaker<String, Short> newEncodeLanguageMaker() {
      return db.treeMap("encode_language", Serializer.STRING, Serializer.SHORT);
    }

    private DB.TreeMapMaker<Short, String> newDecodeLanguageMaker() {
      return db.treeMap("decode_language", Serializer.SHORT, Serializer.STRING);
    }

    private DB.AtomicIntegerMaker newLanguageCounterMaker() {
      return db.atomicInteger("languageCounter");
    }

    @Override
    public Optional<String> getString(long id) {
      return Optional.ofNullable(decodeString.get(id));
    }

    @Override
    public synchronized OptionalLong putString(String str) {
      Long key = encodeString.get(str);
      if (key == null) {
        key = stringCounter.getAndIncrement();
        encodeString.put(str, key);
        decodeString.put(key, str);
      }
      return OptionalLong.of(key);
    }

    @Override
    public Optional<String> getLanguage(short id) {
      return Optional.ofNullable(decodeLanguage.get(id));
    }

    @Override
    public synchronized Optional<Short> putLanguage(String languageTag) {
      Short key = encodeLanguage.get(languageTag);
      if (key == null) {
        key = (short) languageCounter.getAndIncrement();
        encodeLanguage.put(languageTag, key);
        decodeLanguage.put(key, languageTag);
      }
      return Optional.of(key);
    }

    @Override
    public void close() {
      db.close();
    }

  }

  static class StaticMapDBStringStore implements NumericValueFactory.StringStore {

    private final IndexReader<String, Long> encodeString;
    private final IndexReader<Long, String> decodeString;
    private final IndexReader<String, Short> encodeLanguage;
    private final IndexReader<Short, String> decodeLanguage;

    private StaticMapDBStringStore(IndexReader<String, Long> encodeString, IndexReader<Long, String> decodeString, IndexReader<String, Short> encodeLanguage, IndexReader<Short, String> decodeLanguage) {
      this.encodeString = encodeString;
      this.decodeString = decodeString;
      this.encodeLanguage = encodeLanguage;
      this.decodeLanguage = decodeLanguage;
    }

    @Override
    public Optional<String> getString(long id) {
      return Optional.ofNullable(decodeString.get(id));
    }

    @Override
    public OptionalLong putString(String str) {
      Long key = encodeString.get(str);
      return key == null ? OptionalLong.empty() : OptionalLong.of(key);
    }

    @Override
    public Optional<String> getLanguage(short id) {
      return Optional.ofNullable(decodeLanguage.get(id));
    }

    @Override
    public Optional<Short> putLanguage(String languageTag) {
      return Optional.ofNullable(encodeLanguage.get(languageTag));
    }

    @Override
    public void close() {
      encodeString.close();
      decodeString.close();
      encodeLanguage.close();
      decodeLanguage.close();
    }
  }
}
