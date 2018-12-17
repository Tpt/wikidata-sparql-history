package org.wikidata.history.preprocessor;

import org.eclipse.rdf4j.model.IRI;
import org.wikidata.wdtk.datamodel.interfaces.*;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.List;

class WikibaseValueHasher {
  private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();

  private final MessageDigest digest;

  WikibaseValueHasher() {
    try {
      digest = MessageDigest.getInstance("SHA-1");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  String hash(Reference reference) {
    add(reference);
    return digestAndReset();
  }

  String hash(IRI subjectIRI, PropertyIdValue property) {
    add(subjectIRI.stringValue());
    add(property);
    return digestAndReset();
  }

  String hash(Value value) {
    add(value);
    return digestAndReset();
  }

  private void add(Reference reference) {
    List<SnakGroup> groups = reference.getSnakGroups();
    groups.sort(Comparator.comparing(a -> a.getProperty().getId()));
    for (SnakGroup group : groups) {
      //TODO: sort snaks
      for (Snak snak : group) {
        add(snak);
      }
    }
  }

  private void add(Snak snak) {
    if (snak instanceof ValueSnak) {
      add(((ValueSnak) snak));
    } else if (snak instanceof SomeValueSnak) {
      add((SomeValueSnak) snak);
    } else if (snak instanceof NoValueSnak) {
      add((NoValueSnak) snak);
    } else {
      throw new IllegalArgumentException("Unexpected snak type: " + snak);
    }
  }

  private void add(ValueSnak snak) {
    add((byte) 0);
    add(snak.getPropertyId());
    add(snak.getValue());
  }

  private void add(SomeValueSnak snak) {
    add((byte) 1);
    add(snak.getPropertyId());
  }

  private void add(NoValueSnak snak) {
    add((byte) 2);
    add(snak.getPropertyId());
  }

  private void add(Value value) {
    if (value instanceof EntityIdValue) {
      add(((EntityIdValue) value));
    } else if (value instanceof StringValue) {
      add((StringValue) value);
    } else if (value instanceof MonolingualTextValue) {
      add((MonolingualTextValue) value);
    } else if (value instanceof TimeValue) {
      add((TimeValue) value);
    } else if (value instanceof GlobeCoordinatesValue) {
      add((GlobeCoordinatesValue) value);
    } else if (value instanceof QuantityValue) {
      add((QuantityValue) value);
    } else {
      throw new IllegalArgumentException("Unexpected value type: " + value);
    }
  }

  private void add(EntityIdValue value) {
    add(value.getId());
  }

  private void add(MonolingualTextValue value) {
    add(value.getText());
    add(value.getLanguageCode());
  }

  private void add(StringValue value) {
    add(value.getString());

  }

  private void add(TimeValue value) {
    add(value.getYear());
    add(value.getMonth());
    add(value.getDay());
    add(value.getHour());
    add(value.getMinute());
    add(value.getSecond());
    add(value.getPrecision());
    add(value.getTimezoneOffset());
    add(value.getPreferredCalendarModel());
  }

  private void add(GlobeCoordinatesValue value) {
    add(value.getLatitude());
    add(value.getLongitude());
    add(value.getPrecision());
    add(value.getGlobe());
  }


  private void add(QuantityValue value) {
    add(value.getNumericValue());
    add(value.getUpperBound());
    add(value.getLowerBound());
    add(value.getUnit());
  }

  private void add(String value) {
    if (value != null) {
      digest.update(value.getBytes(StandardCharsets.UTF_8));
    }
  }

  private void add(byte value) {
    digest.update(value);
  }

  private void add(int value) {
    for (byte i = 0; i < Integer.BYTES; i++) {
      digest.update((byte) value);
      value >>>= Byte.SIZE;
    }
  }

  private void add(long value) {
    for (byte i = 0; i < Long.BYTES; i++) {
      digest.update((byte) value);
      value >>>= Byte.SIZE;
    }
  }

  private void add(double value) {
    add(Double.doubleToLongBits(value));
  }

  private void add(BigDecimal value) {
    if (value != null) {
      add(value.toString());
    }
  }

  private String digestAndReset() {
    String value = bytesToHex(digest.digest());
    digest.reset();
    return value;
  }

  private static String bytesToHex(byte[] bytes) {
    char[] hexChars = new char[bytes.length * 2];
    for (int j = 0; j < bytes.length; j++) {
      int v = bytes[j] & 0xFF;
      hexChars[j * 2] = HEX_ARRAY[v >>> 4];
      hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
    }
    return new String(hexChars);
  }
}
