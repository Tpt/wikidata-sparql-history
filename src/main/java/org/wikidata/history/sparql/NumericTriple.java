package org.wikidata.history.sparql;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.serializer.GroupSerializer;
import org.mapdb.serializer.GroupSerializerObjectArray;

import java.io.IOException;

final class NumericTriple {
  static final GroupSerializer<NumericTriple> SPOSerializer = new SPOSerializer();
  static final GroupSerializer<NumericTriple> POSSerializer = new POSSerializer();

  private final long subject;
  private final long predicate;
  private final long object;

  NumericTriple(long subject, long predicate, long object) {
    this.subject = subject;
    this.predicate = predicate;
    this.object = object;
  }

  long getSubject() {
    return subject;
  }

  long getPredicate() {
    return predicate;
  }

  long getObject() {
    return object;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof NumericTriple && equals((NumericTriple) o);
  }

  private boolean equals(NumericTriple t) {
    return subject == t.subject && predicate == t.predicate && object == t.object;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(subject) ^ Long.hashCode(object);
  }

  @Override
  public String toString() {
    return String.format("%d %d %d", subject, predicate, object);
  }

  private static class SPOSerializer extends GroupSerializerObjectArray<NumericTriple> {
    @Override
    public void serialize(DataOutput2 out, NumericTriple value) throws IOException {
      out.writeLong(value.subject);
      out.writeLong(value.predicate);
      out.writeLong(value.object);
    }

    @Override
    public NumericTriple deserialize(DataInput2 in, int available) throws IOException {
      long subject = in.readLong();
      long predicate = in.readLong();
      long object = in.readLong();
      return new NumericTriple(subject, predicate, object);
    }

    @Override
    public boolean isTrusted() {
      return true;
    }

    @Override
    public boolean equals(NumericTriple a1, NumericTriple a2) {
      return a1.equals(a2);
    }

    @Override
    public int hashCode(NumericTriple bytes, int seed) {
      return bytes.hashCode();
    }

    @Override
    public int compare(NumericTriple o1, NumericTriple o2) {
      if (o1.subject == o2.subject) {
        if (o1.predicate == o2.predicate) {
          return Long.compare(o1.object, o2.object);
        } else {
          return Long.compare(o1.predicate, o2.predicate);
        }
      } else {
        return Long.compare(o1.subject, o2.subject);
      }
    }

    @Override
    public NumericTriple nextValue(NumericTriple value) {
      if (value.object != 0) {
        return new NumericTriple(value.subject, value.predicate, value.object + 1);
      } else if (value.predicate != 0) {
        return new NumericTriple(value.subject, value.predicate + 1, Long.MIN_VALUE);
      } else {
        return new NumericTriple(value.subject + 1, Long.MIN_VALUE, Long.MIN_VALUE);
      }
    }
  }

  private static class POSSerializer extends GroupSerializerObjectArray<NumericTriple> {
    @Override
    public void serialize(DataOutput2 out, NumericTriple value) throws IOException {
      out.writeLong(value.predicate);
      out.writeLong(value.object);
      out.writeLong(value.subject);

    }

    @Override
    public NumericTriple deserialize(DataInput2 in, int available) throws IOException {
      long predicate = in.readLong();
      long object = in.readLong();
      long subject = in.readLong();
      return new NumericTriple(subject, predicate, object);
    }

    @Override
    public boolean isTrusted() {
      return true;
    }

    @Override
    public boolean equals(NumericTriple a1, NumericTriple a2) {
      return a1.equals(a2);
    }

    @Override
    public int hashCode(NumericTriple bytes, int seed) {
      return bytes.hashCode();
    }

    @Override
    public int compare(NumericTriple o1, NumericTriple o2) {
      if (o1.predicate == o2.predicate) {
        if (o1.object == o2.object) {
          return Long.compare(o1.subject, o2.subject);
        } else {
          return Long.compare(o1.object, o2.object);
        }
      } else {
        return Long.compare(o1.predicate, o2.predicate);
      }
    }

    @Override
    public NumericTriple nextValue(NumericTriple value) {
      if (value.subject != 0) {
        return new NumericTriple(value.subject + 1, value.predicate, value.object);
      } else if (value.object != 0) {
        return new NumericTriple(Long.MIN_VALUE, value.predicate, value.object + 1);
      } else {
        return new NumericTriple(Long.MIN_VALUE, value.predicate + 1, Long.MIN_VALUE);
      }
    }
  }
}
