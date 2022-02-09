package org.wikidata.history.sparql;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class NumericValueFactoryTest {

  @Test
  void testIRIEncoding() throws NotSupportedValueException {
    NumericValueFactory valueFactory = new NumericValueFactory(new TestStringStore());
    testIRIConversion(Vocabulary.WD_NAMESPACE + "Q42", valueFactory);
    testIRIConversion(Vocabulary.WDT_NAMESPACE + "P42", valueFactory);
    testIRIConversion(Vocabulary.P_NAMESPACE + "P42", valueFactory);
    testIRIConversion(Vocabulary.PS_NAMESPACE + "P42", valueFactory);
    testIRIConversion(Vocabulary.PQV_NAMESPACE + "P42", valueFactory);
    testIRIConversion(Vocabulary.REVISION_NAMESPACE + "123", valueFactory);
    Assertions.assertEquals(valueFactory.createIRI(Vocabulary.REVISION_NAMESPACE + "123"), valueFactory.createRevisionIRI(123));
    testIRIConversion(Vocabulary.REVISION_GLOBAL_STATE_NAMESPACE + "123", valueFactory);
    Assertions.assertEquals(valueFactory.createIRI(Vocabulary.REVISION_GLOBAL_STATE_NAMESPACE + "123"), valueFactory.createRevisionIRI(123,Vocabulary.SnapshotType.GLOBAL_STATE));
    testIRIConversion(Vocabulary.REVISION_ADDITIONS_NAMESPACE + "123", valueFactory);
    Assertions.assertEquals(valueFactory.createIRI(Vocabulary.REVISION_ADDITIONS_NAMESPACE + "123"), valueFactory.createRevisionIRI(123,Vocabulary.SnapshotType.ADDITIONS));
    testIRIConversion(Vocabulary.REVISION_DELETIONS_NAMESPACE + "123", valueFactory);
    Assertions.assertEquals(valueFactory.createIRI(Vocabulary.REVISION_DELETIONS_NAMESPACE + "123"), valueFactory.createRevisionIRI(123,Vocabulary.SnapshotType.DELETIONS));
    testIRIConversion("http://example.com", valueFactory);
  }

  @Test
  void testStringEncoding() throws NotSupportedValueException {
    NumericValueFactory valueFactory = new NumericValueFactory(new TestStringStore());
    testStringConversion("foofoofoofoo", valueFactory);
    testStringConversion("bar", valueFactory);
    testLanguageStringConversion("foofoofoofoo", "foofoofoofoo", valueFactory);
    testLanguageStringConversion("bar", "foofoofoofoo", valueFactory);
    testLanguageStringConversion("foofoofoofoo", "en", valueFactory);
    testLanguageStringConversion("bar", "en", valueFactory);
  }

  @Test
  void testNumberEncoding() throws NotSupportedValueException {
    NumericValueFactory valueFactory = new NumericValueFactory(new TestStringStore());
    testIntegerConversion(0, valueFactory);
    testIntegerConversion(1, valueFactory);
    testIntegerConversion(-1, valueFactory);
    testIntegerConversion(Integer.MIN_VALUE, valueFactory);
    testIntegerConversion(Integer.MAX_VALUE, valueFactory);
    testIntegerConversion(Long.MIN_VALUE, valueFactory);
    testIntegerConversion(Long.MAX_VALUE, valueFactory);
  }

  @Test
  void testDateTimeEncoding() throws NotSupportedValueException {
    NumericValueFactory valueFactory = new NumericValueFactory(new TestStringStore());
    testDateTimeConversion("2020-01-01T00:00:00Z", valueFactory);
    testDateTimeConversion("2020-12-31T23:59:60Z", valueFactory);
    testDateTimeConversion("-2020-01-01T00:00:00Z", valueFactory);
    testDateTimeConversion("-10000000000-00-00T00:00:00Z", valueFactory);
  }

  @Test
  void testTypedLiteralEncoding() throws NotSupportedValueException {
    NumericValueFactory valueFactory = new NumericValueFactory(new TestStringStore());
    testTypedLiteralConversion("foofoofoofoo", GEO.WKT_LITERAL, valueFactory);
    testTypedLiteralConversion("foofoofoofoo", XSD.DURATION, valueFactory);

  }

  private void testIRIConversion(String iri, NumericValueFactory valueFactory) throws NotSupportedValueException {
    Assertions.assertEquals(iri,
            valueFactory.createValue(
                    ((NumericValueFactory.NumericValue) valueFactory.createIRI(iri)).encode()
            ).stringValue()
    );
  }

  private void testStringConversion(String str, NumericValueFactory valueFactory) throws NotSupportedValueException {
    Literal value = (Literal) valueFactory.createValue(
            ((NumericValueFactory.NumericValue) valueFactory.createLiteral(str)).encode()
    );
    Assertions.assertEquals(XSD.STRING, value.getDatatype());
    Assertions.assertEquals(str, value.stringValue());
  }

  private void testLanguageStringConversion(String str, String languageCode, NumericValueFactory valueFactory) throws NotSupportedValueException {
    Literal value = (Literal) valueFactory.createValue(
            ((NumericValueFactory.NumericValue) valueFactory.createLiteral(str, languageCode)).encode()
    );
    Assertions.assertEquals(RDF.LANGSTRING, value.getDatatype());
    Assertions.assertEquals(str, value.stringValue());
    Assertions.assertEquals(Optional.of(languageCode), value.getLanguage());
  }

  private void testIntegerConversion(long number, NumericValueFactory valueFactory) throws NotSupportedValueException {
    Literal valueInteger = (Literal) valueFactory.createValue(
            ((NumericValueFactory.NumericValue) valueFactory.createLiteral(number)).encode()
    );
    Assertions.assertEquals(XSD.INTEGER, valueInteger.getDatatype());
    Assertions.assertEquals(number, valueInteger.longValue());

    Literal valueDecimal = (Literal) valueFactory.createValue(
            ((NumericValueFactory.NumericValue) valueFactory.createLiteral(Long.toString(number), XSD.DECIMAL)).encode()
    );
    Assertions.assertEquals(XSD.DECIMAL, valueDecimal.getDatatype());
    Assertions.assertEquals(number, valueDecimal.longValue());
  }

  private void testDateTimeConversion(String time, NumericValueFactory valueFactory) throws NotSupportedValueException {
    Literal value = (Literal) valueFactory.createValue(
            ((NumericValueFactory.NumericValue) valueFactory.createLiteral(time, XSD.DATETIME)).encode()
    );
    Assertions.assertEquals(XSD.DATETIME, value.getDatatype());
    Assertions.assertEquals(time, value.stringValue());
  }

  private void testTypedLiteralConversion(String str, IRI datatype, NumericValueFactory valueFactory) throws NotSupportedValueException {
    Literal value = (Literal) valueFactory.createValue(
            ((NumericValueFactory.NumericValue) valueFactory.createLiteral(str, datatype)).encode()
    );
    Assertions.assertEquals(datatype, value.getDatatype());
    Assertions.assertEquals(str, value.stringValue());
  }

  static class TestStringStore implements NumericValueFactory.StringStore {

    private static final Map<String, Long> ENCODING = new HashMap<>();
    private static final Map<Long, String> DECODING = new HashMap<>();

    static {
      ENCODING.put("foofoofoofoo", 0L);
      DECODING.put(0L, "foofoofoofoo");
      ENCODING.put("bar", (long) Integer.MAX_VALUE);
      DECODING.put((long) Integer.MAX_VALUE, "bar");
      ENCODING.put("en", (long) Short.MAX_VALUE);
      DECODING.put((long) Short.MAX_VALUE, "en");
      ENCODING.put(Long.toString(Long.MAX_VALUE), 1L);
      DECODING.put(1L, Long.toString(Long.MAX_VALUE));
      ENCODING.put(Long.toString(Long.MIN_VALUE), 2L);
      DECODING.put(2L, Long.toString(Long.MIN_VALUE));
      ENCODING.put("http://example.com", 3L);
      DECODING.put(3L, "http://example.com");
    }

    @Override
    public String getString(long id) {
      return DECODING.get(id);
    }

    @Override
    public Long putString(String str) {
      return ENCODING.getOrDefault(str, null);
    }

    @Override
    public String getLanguage(short id) {
      return DECODING.get((long) id);
    }

    @Override
    public Short putLanguage(String languageCode) {
      return ENCODING.containsKey(languageCode) ? ENCODING.get(languageCode).shortValue() : null;
    }

    @Override
    public void close() {
    }
  }
}

