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
  private static final byte[] CONTRIBUTOR_REVISIONS = "contributor_revisions".getBytes();
  private static final byte[] STATEMENT_SPO = "statement_spo".getBytes();
  private static final byte[] STATEMENT_POS = "statement_pos".getBytes();
  private static final byte[] STATEMENT_OSP = "statement_osp".getBytes();
  private static final byte[] STATEMENT_INSERTED = "statement_inserted".getBytes();
  private static final byte[] STATEMENT_DELETED = "statement_deleted".getBytes();
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
          CONTRIBUTOR_REVISIONS,
          STATEMENT_SPO,
          STATEMENT_POS,
          STATEMENT_OSP,
          STATEMENT_INSERTED,
          STATEMENT_DELETED
  };
  private static final byte[] EMPTY_ARRAY = new byte[]{};

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

  Index<Map.Entry<String, Long>, Object> contributorRevisionsIndex() {
    return newIndex(CONTRIBUTOR_REVISIONS, STRING_LONG_SERIALIZER, NULL_SERIALIZER);
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

  Index<Long, long[]> insertedStatementIndex() {
    return newIndex(STATEMENT_INSERTED, LONG_SERIALIZER, LONG_ARRAY_SERIALIZER);
  }

  Index<Long, long[]> deletedStatementIndex() {
    return newIndex(STATEMENT_DELETED, LONG_SERIALIZER, LONG_ARRAY_SERIALIZER);
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

    <E, X extends Exception> CloseableIteration<E, X> longPrefixIteration(long prefix, FailingKVMappingFunction<K, V, E, X> mappingFunction) {
      return prefixIteration(LONG_SERIALIZER.serialize(prefix), mappingFunction);
    }

    <E, X extends Exception> CloseableIteration<E, X> longPrefixIteration(long[] prefix, FailingKVMappingFunction<K, V, E, X> mappingFunction) {
      return prefixIteration(LONG_ARRAY_SERIALIZER.serialize(prefix), mappingFunction);
    }

    <E, X extends Exception> CloseableIteration<E, X> stringPrefixIteration(String prefix, FailingKVMappingFunction<K, V, E, X> mappingFunction) {
      return prefixIteration(STRING_SERIALIZER.serialize(prefix), mappingFunction);
    }

    <E, X extends Exception> CloseableIteration<E, X> prefixIteration(byte[] prefix, FailingKVMappingFunction<K, V, E, X> mappingFunction) {
      try {
        RocksIterator iterator = db.newIterator(columnFamilyHandle);
        iterator.seek(prefix);
        iterator.status();
        return new RocksMappingIteration<>(iterator, prefix, keySerializer, valueSerializer, mappingFunction);
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static final Serializer<Object> NULL_SERIALIZER = new Serializer<>() {
    @Override
    public byte[] serialize(Object value) {
      return EMPTY_ARRAY;
    }

    @Override
    public Object deserialize(byte[] value) {
      return null;
    }
  };

  private static final Serializer<String> STRING_SERIALIZER = new Serializer<>() {
    @Override
    public byte[] serialize(String value) {
      return value.getBytes();
    }

    @Override
    public String deserialize(byte[] value) {
      return new String(value);
    }
  };

  private static final Serializer<Long> LONG_SERIALIZER = new Serializer<>() {
    @Override
    public byte[] serialize(Long value) {
      return Longs.toByteArray(value);
    }

    @Override
    public Long deserialize(byte[] value) {
      return Longs.fromByteArray(value);
    }
  };

  private static final Serializer<long[]> LONG_ARRAY_SERIALIZER = new Serializer<>() {
    @Override
    public byte[] serialize(long[] value) {
      byte[] result = new byte[value.length * 8];
      for (int i = 0; i < value.length; i++) {
        addToArray(result, value[i], 8 * i);
      }
      return result;
    }

    @Override
    public long[] deserialize(byte[] value) {
      int len = value.length / 8;
      long[] result = new long[len];
      for (int i = 0; i < len; i++) {
        result[i] = longFromArray(value, 8 * i);
      }
      return result;
    }
  };

  private static final Serializer<Map.Entry<String, Long>> STRING_LONG_SERIALIZER = new Serializer<>() {
    @Override
    public byte[] serialize(Map.Entry<String, Long> value) {
      byte[] str = value.getKey().getBytes();
      byte[] result = Arrays.copyOf(str, str.length + 8);
      addToArray(result, value.getValue(), str.length);
      return result;
    }

    @Override
    public Map.Entry<String, Long> deserialize(byte[] value) {
      byte[] str = Arrays.copyOf(value, value.length - 8);
      return Pair.of(new String(str), longFromArray(value, value.length - 8));
    }
  };

  private static void addToArray(byte[] array, long value, int offset) {
    for (int j = 7; j >= 0; j--) {
      array[offset + j] = (byte) (value & 0xffL);
      value >>= 8;
    }
  }

  private static long longFromArray(byte[] array, int offset) {
    return Longs.fromBytes(
            array[offset], array[offset + 1], array[offset + 2], array[offset + 3],
            array[offset + 4], array[offset + 5], array[offset + 6], array[offset + 7]
    );
  }

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
    public String getString(long id) {
      try {
        byte[] str = db.get(stringForIdColumnFamilyHandle, Longs.toByteArray(id));
        return (str == null) ? null : new String(str);
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String getLanguage(short id) {
      try {
        byte[] str = db.get(languageForIdColumnFamilyHandle, Shorts.toByteArray(id));
        return (str == null) ? null : new String(str);
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
    public Long putString(String str) {
      byte[] strBytes = str.getBytes();
      try {
        byte[] key = db.get(idForStringColumnFamilyHandle, strBytes);
        return key == null ? null : Longs.fromByteArray(key);
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Short putLanguage(String languageCode) {
      byte[] strBytes = languageCode.getBytes();
      try {
        byte[] key = db.get(idForLanguageColumnFamilyHandle, strBytes);
        return key == null ? null : Shorts.fromByteArray(key);
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
    public synchronized Long putString(String str) {
      byte[] strBytes = str.getBytes();
      try {
        byte[] key = db.get(idForStringColumnFamilyHandle, strBytes);
        if (key == null) {
          key = newStringKey();
          db.put(idForStringColumnFamilyHandle, strBytes, key);
          db.put(stringForIdColumnFamilyHandle, key, strBytes);
        }
        return Longs.fromByteArray(key);
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    private byte[] newStringKey() throws RocksDBException {
      byte[] rawValue = db.get(STRING_COUNTER_NAME);
      long value = rawValue == null ? 0 : Longs.fromByteArray(rawValue);
      db.put(STRING_COUNTER_NAME, Longs.toByteArray(value + 1));
      return Longs.toByteArray(value);
    }

    @Override
    public synchronized Short putLanguage(String str) {
      byte[] strBytes = str.getBytes();
      try {
        byte[] key = db.get(idForLanguageColumnFamilyHandle, strBytes);
        if (key == null) {
          key = newLanguageKey();
          db.put(idForLanguageColumnFamilyHandle, strBytes, key);
          db.put(languageForIdColumnFamilyHandle, key, strBytes);
        }
        return Shorts.fromByteArray(key);
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

  @FunctionalInterface
  interface FailingKVMappingFunction<K, V, E, X extends Exception> {
    E call(K key, V value) throws X;
  }
}
