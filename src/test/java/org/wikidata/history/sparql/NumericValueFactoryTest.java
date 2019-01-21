package org.wikidata.history.sparql;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

public class NumericValueFactoryTest {

  @Test
  public void testIRIEncoding() throws NotSupportedValueException {
    NumericValueFactory valueFactory = new NumericValueFactory(new TestStringStore());
    testIRIConversion(Vocabulary.WD_NAMESPACE + "Q42", valueFactory);
    testIRIConversion(Vocabulary.WDT_NAMESPACE + "P42", valueFactory);
    testIRIConversion(Vocabulary.P_NAMESPACE + "P42", valueFactory);
    testIRIConversion(Vocabulary.PS_NAMESPACE + "P42", valueFactory);
    testIRIConversion(Vocabulary.PQV_NAMESPACE + "P42", valueFactory);
    testIRIConversion(Vocabulary.REVISION_NAMESPACE + "123", valueFactory);
    testIRIConversion(Vocabulary.REVISION_GLOBAL_STATE_NAMESPACE + "123", valueFactory);
    testIRIConversion(Vocabulary.REVISION_ADDITIONS_NAMESPACE + "123", valueFactory);
    testIRIConversion(Vocabulary.REVISION_DELETIONS_NAMESPACE + "123", valueFactory);
    testIRIConversion("http://example.com", valueFactory);
    //TODO: revision IRI
  }

  @Test
  public void testStringEncoding() throws NotSupportedValueException {
    NumericValueFactory valueFactory = new NumericValueFactory(new TestStringStore());
    testStringConversion("foofoofoofoo", valueFactory);
    testStringConversion("bar", valueFactory);
    testLanguageStringConversion("foofoofoofoo", "foofoofoofoo", valueFactory);
    testLanguageStringConversion("bar", "foofoofoofoo", valueFactory);
    testLanguageStringConversion("foofoofoofoo", "en", valueFactory);
    testLanguageStringConversion("bar", "en", valueFactory);
  }

  @Test
  public void testNumberEncoding() throws NotSupportedValueException {
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
  public void testDateTimeEncoding() throws NotSupportedValueException {
    NumericValueFactory valueFactory = new NumericValueFactory(new TestStringStore());
    testDateTimeConversion("2020-01-01T00:00:00Z", valueFactory);
    testDateTimeConversion("2020-12-31T23:59:60Z", valueFactory);
    testDateTimeConversion("-2020-01-01T00:00:00Z", valueFactory);
    testDateTimeConversion("-10000000000-00-00T00:00:00Z", valueFactory);
  }

  @Test
  public void testTypedLiteralEncoding() throws NotSupportedValueException {
    NumericValueFactory valueFactory = new NumericValueFactory(new TestStringStore());
    testTypedLiteralConversion("foofoofoofoo", GEO.WKT_LITERAL, valueFactory);
    testTypedLiteralConversion("foofoofoofoo", XMLSchema.DURATION, valueFactory);

  }

  private void testIRIConversion(String iri, NumericValueFactory valueFactory) throws NotSupportedValueException {
    Assert.assertEquals(iri,
            valueFactory.createValue(
                    ((NumericValueFactory.NumericValue) valueFactory.createIRI(iri)).encode()
            ).stringValue()
    );
  }

  private void testStringConversion(String str, NumericValueFactory valueFactory) throws NotSupportedValueException {
    Literal value = (Literal) valueFactory.createValue(
            ((NumericValueFactory.NumericValue) valueFactory.createLiteral(str)).encode()
    );
    Assert.assertEquals(XMLSchema.STRING, value.getDatatype());
    Assert.assertEquals(str, value.stringValue());
  }

  private void testLanguageStringConversion(String str, String languageCode, NumericValueFactory valueFactory) throws NotSupportedValueException {
    Literal value = (Literal) valueFactory.createValue(
            ((NumericValueFactory.NumericValue) valueFactory.createLiteral(str, languageCode)).encode()
    );
    Assert.assertEquals(RDF.LANGSTRING, value.getDatatype());
    Assert.assertEquals(str, value.stringValue());
    Assert.assertEquals(Optional.of(languageCode), value.getLanguage());
  }

  private void testIntegerConversion(long number, NumericValueFactory valueFactory) throws NotSupportedValueException {
    Literal valueInteger = (Literal) valueFactory.createValue(
            ((NumericValueFactory.NumericValue) valueFactory.createLiteral(number)).encode()
    );
    Assert.assertEquals(XMLSchema.INTEGER, valueInteger.getDatatype());
    Assert.assertEquals(number, valueInteger.longValue());

    Literal valueDecimal = (Literal) valueFactory.createValue(
            ((NumericValueFactory.NumericValue) valueFactory.createLiteral(Long.toString(number), XMLSchema.DECIMAL)).encode()
    );
    Assert.assertEquals(XMLSchema.DECIMAL, valueDecimal.getDatatype());
    Assert.assertEquals(number, valueDecimal.longValue());
  }

  private void testDateTimeConversion(String time, NumericValueFactory valueFactory) throws NotSupportedValueException {
    Literal value = (Literal) valueFactory.createValue(
            ((NumericValueFactory.NumericValue) valueFactory.createLiteral(time, XMLSchema.DATETIME)).encode()
    );
    Assert.assertEquals(XMLSchema.DATETIME, value.getDatatype());
    Assert.assertEquals(time, value.stringValue());
  }

  private void testTypedLiteralConversion(String str, IRI datatype, NumericValueFactory valueFactory) throws NotSupportedValueException {
    Literal value = (Literal) valueFactory.createValue(
            ((NumericValueFactory.NumericValue) valueFactory.createLiteral(str, datatype)).encode()
    );
    Assert.assertEquals(datatype, value.getDatatype());
    Assert.assertEquals(str, value.stringValue());
  }

  static class TestStringStore implements NumericValueFactory.StringStore {

    private static Map<String, Long> ENCODING = new HashMap<>();
    private static Map<Long, String> DECODING = new HashMap<>();

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
    public Optional<String> getString(long id) {
      return Optional.ofNullable(DECODING.get(id));
    }

    @Override
    public OptionalLong putString(String str) {
      return ENCODING.containsKey(str) ? OptionalLong.of(ENCODING.get(str)) : OptionalLong.empty();
    }

    @Override
    public Optional<String> getLanguage(short id) {
      return Optional.ofNullable(DECODING.get((long) id));
    }

    @Override
    public Optional<Short> putLanguage(String languageCode) {
      return ENCODING.containsKey(languageCode) ? Optional.of(ENCODING.get(languageCode).shortValue()) : Optional.empty();
    }

    @Override
    public void close() {
    }
  }
}

