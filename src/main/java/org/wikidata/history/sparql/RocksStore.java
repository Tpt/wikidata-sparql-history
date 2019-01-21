package org.wikidata.history.sparql;

import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.rocksdb.*;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class RocksStore implements AutoCloseable {
  private static final byte[] STRING_COUNTER_NAME = "stringCounter".getBytes();
  private static final byte[] LANGUAGE_COUNTER_NAME = "languageCounter".getBytes();
  private static final byte[] ID_FOR_STR_COLUMN_NAME = "id4str".getBytes();
  private static final byte[] STR_FOR_ID_COLUMN_NAME = "str4id".getBytes();
  private static final byte[] ID_FOR_LANGUAGE_COLUMN_NAME = "id4lang".getBytes();
  private static final byte[] LANGUAGE_FOR_ID_COLUMN_NAME = "lang4id".getBytes();
  private static final byte[] REVISION_DATE = "revision_date".getBytes();
  private static final byte[] DATE_REVISIONS = "date_revisions".getBytes();
  private static final byte[] PARENT_REVISION = "parent_revision".getBytes();
  private static final byte[] CHILD_REVISION = "child_revision".getBytes();
  private static final byte[] REVISION_TOPIC = "revision_topic".getBytes();
  private static final byte[] TOPIC_REVISION = "topic_revision".getBytes();
  private static final byte[] REVISION_CONTRIBUTOR = "revision_contributor".getBytes();
  private static final byte[] STATEMENT_SPO = "statement_spo".getBytes();
  private static final byte[] STATEMENT_POS = "statement_pos".getBytes();
  private static final byte[] STATEMENT_OSP = "statement_osp".getBytes();
  private static final byte[][] COLUMN_FAMILIES = new byte[][]{
          RocksDB.DEFAULT_COLUMN_FAMILY,
          ID_FOR_STR_COLUMN_NAME,
          STR_FOR_ID_COLUMN_NAME,
          ID_FOR_LANGUAGE_COLUMN_NAME,
          LANGUAGE_FOR_ID_COLUMN_NAME,
          REVISION_DATE,
          DATE_REVISIONS,
          PARENT_REVISION,
          CHILD_REVISION,
          REVISION_TOPIC,
          TOPIC_REVISION,
          REVISION_CONTRIBUTOR,
          STATEMENT_SPO,
          STATEMENT_POS,
          STATEMENT_OSP
  };

  private final ColumnFamilyOptions columnFamilyOptions;
  private final Map<byte[], ColumnFamilyHandle> columnFamilyHandles = new HashMap<>();
  private final RStringStore rStringStore;
  private final RWStringStore rwStringStore;
  private final DBOptions options;
  private final RocksDB db;

  public RocksStore(Path dbPath, boolean readOnly) {
    columnFamilyOptions = new ColumnFamilyOptions()
            .optimizeUniversalStyleCompaction()
            .setCompressionType(CompressionType.LZ4HC_COMPRESSION)
            .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
            .setLevelCompactionDynamicLevelBytes(true);
    List<ColumnFamilyDescriptor> columnFamilyDescriptors = Arrays.stream(COLUMN_FAMILIES)
            .map(name -> new ColumnFamilyDescriptor(name, columnFamilyOptions))
            .collect(Collectors.toList());
    final List<ColumnFamilyHandle> columnFamilyHandlesList = new ArrayList<>();
    options = new DBOptions()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true)
            .setWalTtlSeconds(60 * 60 * 24)
            .setWalSizeLimitMB(4096);
    try {
      if (readOnly) {
        db = RocksDB.openReadOnly(options, dbPath.toString(), columnFamilyDescriptors, columnFamilyHandlesList);
      } else {
        db = RocksDB.open(options, dbPath.toString(), columnFamilyDescriptors, columnFamilyHandlesList);
      }
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }

    // We setup column families map
    assert columnFamilyHandlesList.size() == COLUMN_FAMILIES.length;
    for (int i = 0; i < COLUMN_FAMILIES.length; i++) {
      columnFamilyHandles.put(COLUMN_FAMILIES[i], columnFamilyHandlesList.get(i));
    }

    rStringStore = new RStringStore(db,
            columnFamilyHandles.get(STR_FOR_ID_COLUMN_NAME), columnFamilyHandles.get(ID_FOR_STR_COLUMN_NAME),
            columnFamilyHandles.get(LANGUAGE_FOR_ID_COLUMN_NAME), columnFamilyHandles.get(ID_FOR_LANGUAGE_COLUMN_NAME));
    rwStringStore = new RWStringStore(db,
            columnFamilyHandles.get(STR_FOR_ID_COLUMN_NAME), columnFamilyHandles.get(ID_FOR_STR_COLUMN_NAME),
            columnFamilyHandles.get(LANGUAGE_FOR_ID_COLUMN_NAME), columnFamilyHandles.get(ID_FOR_LANGUAGE_COLUMN_NAME));
  }

  NumericValueFactory.StringStore getReadOnlyStringStore() {
    return rStringStore;
  }

  NumericValueFactory.StringStore getReadWriteStringStore() {
    return rwStringStore;
  }

  Index<Long, Long> revisionDateIndex() {
    return newIndex(REVISION_DATE, LONG_SERIALIZER, LONG_SERIALIZER);
  }

  Index<Long, long[]> dateRevisionsIndex() {
    return newIndex(DATE_REVISIONS, LONG_SERIALIZER, LONG_ARRAY_SERIALIZER);
  }

  Index<Long, Long> parentRevisionIndex() {
    return newIndex(PARENT_REVISION, LONG_SERIALIZER, LONG_SERIALIZER);
  }

  Index<Long, Long> childRevisionIndex() {
    return newIndex(CHILD_REVISION, LONG_SERIALIZER, LONG_SERIALIZER);
  }

  Index<Long, Long> revisionTopicIndex() {
    return newIndex(REVISION_TOPIC, LONG_SERIALIZER, LONG_SERIALIZER);
  }

  Index<Long, long[]> topicRevisionIndex() {
    return newIndex(TOPIC_REVISION, LONG_SERIALIZER, LONG_ARRAY_SERIALIZER);
  }

  Index<Long, String> revisionContributorIndex() {
    return newIndex(REVISION_CONTRIBUTOR, LONG_SERIALIZER, STRING_SERIALIZER);
  }

  Index<long[], long[]> spoStatementIndex() {
    return newIndex(STATEMENT_SPO, LONG_ARRAY_SERIALIZER, LONG_ARRAY_SERIALIZER);
  }

  Index<long[], long[]> posStatementIndex() {
    return newIndex(STATEMENT_POS, LONG_ARRAY_SERIALIZER, LONG_ARRAY_SERIALIZER);
  }

  Index<long[], long[]> ospStatementIndex() {
    return newIndex(STATEMENT_OSP, LONG_ARRAY_SERIALIZER, LONG_ARRAY_SERIALIZER);
  }

  private <K, V> Index<K, V> newIndex(byte[] columnName, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    return new Index<>(db, columnFamilyHandles.get(columnName), keySerializer, valueSerializer);
  }

  public void compact() {
    try {
      db.compactRange();
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles.values()) {
      columnFamilyHandle.close();
    }
    db.close();
    options.close();
    columnFamilyOptions.close();
  }

  interface Serializer<T> {
    byte[] serialize(T value);

    T deserialize(byte[] value);
  }

  public static class Index<K, V> {
    private final RocksDB db;
    private final ColumnFamilyHandle columnFamilyHandle;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    private Index(RocksDB db, ColumnFamilyHandle columnFamilyHandle, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
      this.db = db;
      this.columnFamilyHandle = columnFamilyHandle;
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
    }

    public boolean containsKey(K k) {
      try {
        return db.get(columnFamilyHandle, keySerializer.serialize(k)) != null;
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    public V get(K k) {
      try {
        byte[] rawValue = db.get(columnFamilyHandle, keySerializer.serialize(k));
        return rawValue == null ? null : valueSerializer.deserialize(rawValue);
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    public V getOrDefault(K k, V def) {
      V result = get(k);
      return result == null ? def : result;
    }

    public void put(K k, V v) {
      try {
        db.put(columnFamilyHandle, keySerializer.serialize(k), valueSerializer.serialize(v));
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    public void remove(K k) {
      try {
        db.delete(columnFamilyHandle, keySerializer.serialize(k));
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    <E, X extends Exception> CloseableIteration<E, X> longPrefixIteration(long[] prefix, FailingKVMappingFunction<K, V, E, X> mappingFunction) {
      RocksIterator iterator = db.newIterator(columnFamilyHandle);
      byte[] rawPrefix = LONG_ARRAY_SERIALIZER.serialize(prefix);
      iterator.seek(rawPrefix);
      return new RocksMappingIteration<>(iterator, rawPrefix, keySerializer, valueSerializer, mappingFunction);
    }

    Iterator<Map.Entry<K, V>> longPrefixIterator(long[] prefix) {
      RocksIterator iterator = db.newIterator(columnFamilyHandle);
      byte[] rawPrefix = LONG_ARRAY_SERIALIZER.serialize(prefix);
      iterator.seek(rawPrefix);
      return new RocksSimpleIterator<>(iterator, rawPrefix, keySerializer, valueSerializer);
    }
  }

  private static final Serializer<String> STRING_SERIALIZER = new Serializer<String>() {
    @Override
    public byte[] serialize(String value) {
      return value.getBytes();
    }

    @Override
    public String deserialize(byte[] value) {
      return new String(value);
    }
  };

  private static final Serializer<Long> LONG_SERIALIZER = new Serializer<Long>() {
    @Override
    public byte[] serialize(Long value) {
      return Longs.toByteArray(value);
    }

    @Override
    public Long deserialize(byte[] value) {
      return Longs.fromByteArray(value);
    }
  };

  static final Serializer<long[]> LONG_ARRAY_SERIALIZER = new Serializer<long[]>() {
    @Override
    public byte[] serialize(long[] value) {
      byte[] result = new byte[value.length * 8];
      for (int i = 0; i < value.length; i++) {
        long val = value[i];
        for (int j = 7; j >= 0; j--) {
          result[8 * i + j] = (byte) (val & 0xffL);
          val >>= 8;
        }
      }
      return result;
    }

    @Override
    public long[] deserialize(byte[] value) {
      long[] result = new long[value.length / 8];
      for (int offset = 0; offset < value.length; offset += 8) {
        result[offset / 8] = Longs.fromBytes(
                value[offset], value[offset + 1], value[offset + 2], value[offset + 3],
                value[offset + 4], value[offset + 5], value[offset + 6], value[offset + 7]
        );
      }
      return result;
    }
  };

  private static abstract class BasicStringStore implements NumericValueFactory.StringStore {
    protected final RocksDB db;
    protected final ColumnFamilyHandle stringForIdColumnFamilyHandle;
    protected final ColumnFamilyHandle idForStringColumnFamilyHandle;
    protected final ColumnFamilyHandle languageForIdColumnFamilyHandle;
    protected final ColumnFamilyHandle idForLanguageColumnFamilyHandle;

    BasicStringStore(RocksDB db, ColumnFamilyHandle stringForIdColumnFamilyHandle, ColumnFamilyHandle idForStringColumnFamilyHandle, ColumnFamilyHandle languageForIdColumnFamilyHandle, ColumnFamilyHandle idForLanguageColumnFamilyHandle) {
      this.db = db;
      this.stringForIdColumnFamilyHandle = stringForIdColumnFamilyHandle;
      this.idForStringColumnFamilyHandle = idForStringColumnFamilyHandle;
      this.languageForIdColumnFamilyHandle = languageForIdColumnFamilyHandle;
      this.idForLanguageColumnFamilyHandle = idForLanguageColumnFamilyHandle;
    }

    @Override
    public Optional<String> getString(long id) {
      try {
        return Optional.ofNullable(db.get(stringForIdColumnFamilyHandle, Longs.toByteArray(id))).map(String::new);
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Optional<String> getLanguage(short id) {
      try {
        return Optional.ofNullable(db.get(languageForIdColumnFamilyHandle, Shorts.toByteArray(id))).map(String::new);
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {
    }
  }

  private static class RStringStore extends BasicStringStore {
    RStringStore(RocksDB db, ColumnFamilyHandle stringForIdColumnFamilyHandle, ColumnFamilyHandle idForStringColumnFamilyHandle, ColumnFamilyHandle languageForIdColumnFamilyHandle, ColumnFamilyHandle idForLanguageColumnFamilyHandle) {
      super(db, stringForIdColumnFamilyHandle, idForStringColumnFamilyHandle, languageForIdColumnFamilyHandle, idForLanguageColumnFamilyHandle);
    }

    @Override
    public OptionalLong putString(String str) {
      byte[] strBytes = str.getBytes();
      try {
        byte[] key = db.get(idForStringColumnFamilyHandle, strBytes);
        return key == null ? OptionalLong.empty() : OptionalLong.of(Longs.fromByteArray(key));
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Optional<Short> putLanguage(String languageCode) {
      byte[] strBytes = languageCode.getBytes();
      try {
        return Optional.ofNullable(db.get(idForLanguageColumnFamilyHandle, strBytes)).map(Shorts::fromByteArray);
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class RWStringStore extends BasicStringStore {
    RWStringStore(RocksDB db, ColumnFamilyHandle stringForIdColumnFamilyHandle, ColumnFamilyHandle idForStringColumnFamilyHandle, ColumnFamilyHandle languageForIdColumnFamilyHandle, ColumnFamilyHandle idForLanguageColumnFamilyHandle) {
      super(db, stringForIdColumnFamilyHandle, idForStringColumnFamilyHandle, languageForIdColumnFamilyHandle, idForLanguageColumnFamilyHandle);
    }

    @Override
    public OptionalLong putString(String str) {
      byte[] strBytes = str.getBytes();
      try {
        byte[] key = db.get(idForStringColumnFamilyHandle, strBytes);
        if (key == null) {
          key = newStringKey();
          db.put(idForStringColumnFamilyHandle, strBytes, key);
          db.put(stringForIdColumnFamilyHandle, key, strBytes);
        }
        return OptionalLong.of(Longs.fromByteArray(key));
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    private synchronized byte[] newStringKey() throws RocksDBException {
      byte[] rawValue = db.get(STRING_COUNTER_NAME);
      long value = rawValue == null ? 0 : Longs.fromByteArray(rawValue);
      db.put(STRING_COUNTER_NAME, Longs.toByteArray(value + 1));
      return Longs.toByteArray(value);
    }

    @Override
    public Optional<Short> putLanguage(String str) {
      byte[] strBytes = str.getBytes();
      try {
        byte[] key = db.get(idForLanguageColumnFamilyHandle, strBytes);
        if (key == null) {
          key = newLanguageKey();
          db.put(idForLanguageColumnFamilyHandle, strBytes, key);
          db.put(languageForIdColumnFamilyHandle, key, strBytes);
        }
        return Optional.of(Shorts.fromByteArray(key));
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    private synchronized byte[] newLanguageKey() throws RocksDBException {
      byte[] rawValue = db.get(LANGUAGE_COUNTER_NAME);
      short value = rawValue == null ? 0 : Shorts.fromByteArray(rawValue);
      db.put(LANGUAGE_COUNTER_NAME, Shorts.toByteArray((short) (value + 1)));
      return Shorts.toByteArray(value);
    }
  }

  private static class RocksMappingIteration<K, V, E, X extends Exception> implements CloseableIteration<E, X> {
    private final RocksIterator iterator;
    private final byte[] prefix;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final FailingKVMappingFunction<K, V, E, X> mappingFunction;


    private RocksMappingIteration(RocksIterator iterator, byte[] prefix, Serializer<K> keySerializer, Serializer<V> valueSerializer, FailingKVMappingFunction<K, V, E, X> mappingFunction) {
      this.iterator = iterator;
      this.prefix = prefix;
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
      this.mappingFunction = mappingFunction;

      //We start the iteration
      iterator.next();
    }

    @Override
    public boolean hasNext() {
      return iterator.isValid() && hasPrefix();
    }

    @Override
    public E next() throws X {
      if (!hasNext()) {
        throw new NoSuchElementException("The iterator is finished");
      }
      try {
        return mappingFunction.call(
                keySerializer.deserialize(iterator.key()),
                valueSerializer.deserialize(iterator.value())
        );
      } finally {
        iterator.next();
      }
    }

    private boolean hasPrefix() {
      byte[] key = iterator.key();
      if (key.length < prefix.length) {
        return false;
      }
      for (int i = 0; i < prefix.length; i++) {
        if (key[i] != prefix[i]) {
          return false;
        }
      }
      return true;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
      iterator.close();
    }
  }

  private static class RocksSimpleIterator<K, V> implements Iterator<Map.Entry<K, V>> {
    private final RocksIterator iterator;
    private final byte[] prefix;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;


    private RocksSimpleIterator(RocksIterator iterator, byte[] prefix, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
      this.iterator = iterator;
      this.prefix = prefix;
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;

      //We start the iteration
      iterator.next();
    }

    @Override
    public boolean hasNext() {
      boolean result = iterator.isValid() && hasPrefix();
      if (!result) {
        iterator.close();
      }
      return result;
    }

    @Override
    public Map.Entry<K, V> next() {
      if (!hasNext()) {
        throw new NoSuchElementException("The iterator is finished");
      }
      try {
        return Pair.of(
                keySerializer.deserialize(iterator.key()),
                valueSerializer.deserialize(iterator.value())
        );
      } finally {
        iterator.next();
      }
    }

    private boolean hasPrefix() {
      byte[] key = iterator.key();
      if (key.length < prefix.length) {
        return false;
      }
      for (int i = 0; i < prefix.length; i++) {
        if (key[i] != prefix[i]) {
          return false;
        }
      }
      return true;
    }
  }

  @FunctionalInterface
  interface FailingKVMappingFunction<K, V, E, X extends Exception> {
    E call(K key, V value) throws X;
  }
}
