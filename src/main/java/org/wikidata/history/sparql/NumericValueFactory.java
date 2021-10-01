package org.wikidata.history.sparql;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.base.AbstractValueFactory;
import org.eclipse.rdf4j.model.util.URIUtil;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * TODO: improve datatype encoding
 */
final class NumericValueFactory extends AbstractValueFactory implements AutoCloseable {

  private static final Pattern TIME_STRING_PATTERN = Pattern.compile("^([+-]?\\d{4,})-(\\d{2})-(\\d{2})T(\\d{2}):(\\d{2}):(\\d{2})Z$");
  private static final Logger LOGGER = LoggerFactory.getLogger(NumericValueFactory.class);

  private static final long TYPE_SHIFT = 12;
  private static final byte IRI_TYPE = 1;
  private static final byte BLANK_NODE_TYPE = 2;
  private static final byte LITERAL_TYPE = 3;
  private static final byte LANGUAGE_STRING_TYPE = 4;
  private static final byte ITEM_ID_TYPE = 5;
  private static final byte PROPERTY_ID_TYPE = 6;
  private static final byte TIME_TYPE = 7;
  private static final byte SMALL_LONG_DECIMAL_TYPE = 8;
  private static final byte SMALL_LONG_INTEGER_TYPE = 9;
  private static final byte SMALL_STRING_TYPE = 10;
  private static final byte REVISION_ID_TYPE = 11;
  private static final long LANGUAGE_TAG_SHIFT = Short.MAX_VALUE + 1;
  private static final long DATATYPE_SHIFT = Short.MAX_VALUE + 1;
  private static final long PROPERTY_TYPE_SHIFT = 32;
  private static final long SNAPSHOT_TYPE_SHIFT = 4;
  private static final long MAX_ENCODED_VALUE = Long.MAX_VALUE / TYPE_SHIFT;
  private static final long MIN_ENCODED_VALUE = Long.MIN_VALUE / TYPE_SHIFT;

  private static final IRI[] DATATYPES = new IRI[]{
          XSD.STRING,
          XSD.BOOLEAN,
          XSD.DECIMAL,
          XSD.INTEGER,
          XSD.DOUBLE,
          XSD.FLOAT,
          XSD.DATE,
          XSD.TIME,
          XSD.DATETIME,
          XSD.GYEAR,
          XSD.GMONTH,
          XSD.GDAY,
          XSD.GYEARMONTH,
          XSD.GMONTHDAY,
          XSD.DURATION,
          XSD.YEARMONTHDURATION,
          XSD.DAYTIMEDURATION,
          XSD.BYTE,
          XSD.SHORT,
          XSD.INT,
          XSD.LONG,
          XSD.UNSIGNED_BYTE,
          XSD.UNSIGNED_SHORT,
          XSD.UNSIGNED_INT,
          XSD.UNSIGNED_LONG,
          XSD.POSITIVE_INTEGER,
          XSD.NON_NEGATIVE_INTEGER,
          XSD.NEGATIVE_INTEGER,
          XSD.NON_POSITIVE_INTEGER,
          XSD.HEXBINARY,
          XSD.BASE64BINARY,
          XSD.ANYURI,
          XSD.LANGUAGE,
          XSD.NORMALIZEDSTRING,
          XSD.TOKEN,
          XSD.NMTOKEN,
          XSD.NAME,
          XSD.NCNAME,
          RDF.HTML,
          RDF.XMLLITERAL,
          GEO.WKT_LITERAL
  };

  private static final Map<String, Short> DATATYPE_ENCODING = new HashMap<>();

  static {
    for (short i = 0; i < DATATYPES.length; i++) {
      DATATYPE_ENCODING.put(DATATYPES[i].stringValue(), i);
    }
  }

  private static final DatatypeFactory DATATYPE_FACTORY;

  static {
    try {
      DATATYPE_FACTORY = DatatypeFactory.newInstance();
    } catch (DatatypeConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

  private final StringStore stringStore;

  NumericValueFactory(StringStore stringStore) {
    this.stringStore = stringStore;
  }

  long encodeValue(Value value) throws NotSupportedValueException {
    if (!(value instanceof NumericValue)) {
      if (value instanceof IRI) {
        value = createIRI((IRI) value);
      } else if (value instanceof BNode) {
        value = createBNode((BNode) value);
      } else if (value instanceof Literal) {
        value = createLiteral((Literal) value);
      }
    }
    if (!(value instanceof NumericValue)) {
      throw new NotSupportedValueException("Not able to numerically encode: " + value);
    }
    return ((NumericValue) value).encode();
  }

  Value createValue(long value) throws NotSupportedValueException {
    byte type = (byte) Math.abs(value % TYPE_SHIFT);
    value /= TYPE_SHIFT;
    switch (type) {
      case IRI_TYPE:
        return new DictionaryIRI(value, stringStore);
      case BLANK_NODE_TYPE:
        return new DictionaryBNode(value, stringStore);
      case LITERAL_TYPE:
        return new DictionaryLiteral(value, stringStore);
      case LANGUAGE_STRING_TYPE:
        return new DictionaryLanguageTaggedString(value, stringStore);
      case ITEM_ID_TYPE:
        return new ItemIRI(value);
      case PROPERTY_ID_TYPE:
        return new PropertyIRI(value);
      case SMALL_STRING_TYPE:
        return new SmallStringLiteral(value);
      case TIME_TYPE:
        return new TimeLiteral(value);
      case SMALL_LONG_DECIMAL_TYPE:
        return new SmallLongDecimalLiteral(value);
      case SMALL_LONG_INTEGER_TYPE:
        return new SmallLongIntegerLiteral(value);
      case REVISION_ID_TYPE:
        return new RevisionIRI(value);
      default:
        throw new NotSupportedValueException("Unknown type id: " + type);
    }
  }

  @Override
  public IRI createIRI(String iri) {
    int localNameIdx = URIUtil.getLocalNameIndex(iri);
    return createIRI(iri.substring(0, localNameIdx), iri.substring(localNameIdx));
  }

  @Override
  public IRI createIRI(String namespace, String localName) {
    if (Vocabulary.WD_NAMESPACE.equals(namespace) && !localName.isEmpty()) {
      if (localName.charAt(0) == 'Q') {
        return new ItemIRI(Long.parseLong(localName.substring(1)));
      } else if (localName.charAt(0) == 'P') {
        return new PropertyIRI(Long.parseLong(localName.substring(1)), PropertyType.ENTITY);
      }
    }
    if (Vocabulary.WDT_NAMESPACE.equals(namespace)) {
      return new PropertyIRI(Long.parseLong(localName.substring(1)), PropertyType.PROP_DIRECT);
    }
    if (Vocabulary.P_NAMESPACE.equals(namespace)) {
      return new PropertyIRI(Long.parseLong(localName.substring(1)), PropertyType.PROP);
    }
    if (Vocabulary.WDNO_NAMESPACE.equals(namespace)) {
      return new PropertyIRI(Long.parseLong(localName.substring(1)), PropertyType.PROP_NOVALUE);
    }
    if (Vocabulary.PS_NAMESPACE.equals(namespace)) {
      return new PropertyIRI(Long.parseLong(localName.substring(1)), PropertyType.PROP_STATEMENT);
    }
    if (Vocabulary.PSV_NAMESPACE.equals(namespace)) {
      return new PropertyIRI(Long.parseLong(localName.substring(1)), PropertyType.PROP_STATEMENT_VALUE);
    }
    if (Vocabulary.PQ_NAMESPACE.equals(namespace)) {
      return new PropertyIRI(Long.parseLong(localName.substring(1)), PropertyType.PROP_QUALIFIER);
    }
    if (Vocabulary.PQV_NAMESPACE.equals(namespace)) {
      return new PropertyIRI(Long.parseLong(localName.substring(1)), PropertyType.PROP_QUALIFIER_VALUE);
    }
    if (Vocabulary.PR_NAMESPACE.equals(namespace)) {
      return new PropertyIRI(Long.parseLong(localName.substring(1)), PropertyType.PROP_REFERENCE);
    }
    if (Vocabulary.PRV_NAMESPACE.equals(namespace)) {
      return new PropertyIRI(Long.parseLong(localName.substring(1)), PropertyType.PROP_REFERENCE_VALUE);
    }
    if (Vocabulary.REVISION_NAMESPACE.equals(namespace)) {
      return new RevisionIRI(Long.parseLong(localName), Vocabulary.SnapshotType.NONE);
    }
    if (Vocabulary.REVISION_GLOBAL_STATE_NAMESPACE.equals(namespace)) {
      return new RevisionIRI(Long.parseLong(localName), Vocabulary.SnapshotType.GLOBAL_STATE);
    }
    if (Vocabulary.REVISION_ADDITIONS_NAMESPACE.equals(namespace)) {
      return new RevisionIRI(Long.parseLong(localName), Vocabulary.SnapshotType.ADDITIONS);
    }
    if (Vocabulary.REVISION_DELETIONS_NAMESPACE.equals(namespace)) {
      return new RevisionIRI(Long.parseLong(localName), Vocabulary.SnapshotType.DELETIONS);
    }

    String iri = namespace + localName;
    Long encodedIri = stringStore.putString(iri);
    if (encodedIri != null) {
      return new DictionaryIRI(encodedIri, iri);
    } else {
      return super.createIRI(iri);
    }
  }

  Value createValue(Value value) {
    if (value instanceof IRI) {
      return createIRI((IRI) value);
    } else if (value instanceof BNode) {
      return createBNode((BNode) value);
    } else if (value instanceof Literal) {
      return createLiteral((Literal) value);
    } else {
      return value;
    }
  }

  Resource createResource(Resource resource) {
    if (resource instanceof IRI) {
      return createIRI((IRI) resource);
    } else if (resource instanceof BNode) {
      return createBNode((BNode) resource);
    } else {
      return resource;
    }
  }

  IRI createIRI(IRI iri) {
    if (iri instanceof NumericValue) {
      return iri;
    }
    return createIRI(iri.getNamespace(), iri.getLocalName());
  }

  @Override
  public BNode createBNode(String nodeID) {
    Long encodedId = stringStore.putString(nodeID);
    if (encodedId != null) {
      return new DictionaryBNode(encodedId, nodeID);
    } else {
      return super.createBNode(nodeID);
    }
  }

  BNode createBNode(BNode node) {
    if (node instanceof NumericValue) {
      return node;
    }
    return createBNode(node.getID());
  }

  @Override
  public Literal createLiteral(String label) {
    if (label.length() <= 7) {
      byte[] encoding = label.getBytes();
      if (encoding.length <= 7) {
        return new SmallStringLiteral(bytesToLong(encoding));
      }
    }
    return createDictionaryLiteral(label, XSD.STRING);
  }

  @Override
  public Literal createLiteral(String label, String language) {
    Long encodedLabel = stringStore.putString(label);
    Short encodedLanguage = stringStore.putLanguage(language);
    if (encodedLabel != null && encodedLanguage != null) {
      return new DictionaryLanguageTaggedString(encodedLabel, encodedLanguage, label);
    } else {
      return super.createLiteral(label, language);
    }
  }

  @Override
  public Literal createLiteral(String label, IRI datatype) {
    if (XSD.STRING.equals(datatype)) {
      return createLiteral(label);
    } else if (XSD.DATETIME.equals(datatype)) {
      return createDateTime(label);
    } else if (GEO.WKT_LITERAL.equals(datatype)) {
      return createDictionaryLiteral(label, datatype);
    } else if (XSD.INTEGER.equals(datatype)) {
      try {
        long value = XMLDatatypeUtil.parseLong(label);
        if (MIN_ENCODED_VALUE < value && value < MAX_ENCODED_VALUE) {
          return new SmallLongIntegerLiteral(value);
        } else {
          return createDictionaryLiteral(label, datatype);
        }
      } catch (NumberFormatException e) {
        return createDictionaryLiteral(label, datatype);
      }
    } else if (XSD.DECIMAL.equals(datatype)) {
      try {
        long value = XMLDatatypeUtil.parseLong(label);
        if (MIN_ENCODED_VALUE < value && value < MAX_ENCODED_VALUE) {
          return new SmallLongDecimalLiteral(value);
        } else {
          return createDictionaryLiteral(label, datatype);
        }
      } catch (NumberFormatException e) {
        return createDictionaryLiteral(label, datatype);
      }
    } else {
      return createDictionaryLiteral(label, datatype);
    }
  }

  private Literal createDictionaryLiteral(String label, IRI datatype) {
    Long encodedLabel = stringStore.putString(label);
    if (encodedLabel != null) {
      Short datatypeId = DATATYPE_ENCODING.get(datatype.stringValue());
      if (datatypeId != null) {
        return new DictionaryLiteral(encodedLabel, datatypeId, label);
      }
    }
    return super.createLiteral(label, datatype);
  }

  private static long bytesToLong(byte[] b) {
    long result = 0;
    for (int i = b.length - 1; i >= 0; i--) {
      result <<= 8;
      result |= (b[i] & 0xFF);
    }
    return result;
  }

  private Literal createDateTime(String value) throws IllegalArgumentException {
    Matcher matcher = TIME_STRING_PATTERN.matcher(value);
    if (!matcher.matches()) {
      LOGGER.warn("Not valid time value: " + value);
      return createDictionaryLiteral(value, XSD.DATETIME);
    }
    long year = XMLDatatypeUtil.parseLong(matcher.group(1));
    long month = Long.parseLong(matcher.group(2));
    long day = Long.parseLong(matcher.group(3));
    long hours = Long.parseLong(matcher.group(4));
    long minutes = Long.parseLong(matcher.group(5));
    long seconds = Long.parseLong(matcher.group(6));
    long encoded = compose(compose(compose(compose(compose(year, 13, month), 32, day), 25, hours), 62, minutes), 62, seconds);
    if (encoded >= MAX_ENCODED_VALUE) {
      LOGGER.warn("Too big time value: " + value);
      return createDictionaryLiteral(value, XSD.DATETIME);
    }
    return new TimeLiteral(encoded);
  }

  @Override
  public Literal createLiteral(byte value) {
    return new SmallLongIntegerLiteral(value);
  }

  @Override
  public Literal createLiteral(short value) {
    return new SmallLongIntegerLiteral(value);
  }

  @Override
  public Literal createLiteral(int value) {
    return new SmallLongIntegerLiteral(value);
  }

  @Override
  public Literal createLiteral(long value) {
    if (MIN_ENCODED_VALUE < value && value < MAX_ENCODED_VALUE) {
      return new SmallLongIntegerLiteral(value);
    } else {
      return createDictionaryLiteral(Long.toString(value), XSD.INTEGER);
    }
  }

  @Override
  public Literal createLiteral(BigInteger value) {
    try {
      return createLiteral(value.longValueExact());
    } catch (ArithmeticException e) {
      return createDictionaryLiteral(value.toString(), XSD.INTEGER);
    }
  }

  @Override
  public Literal createLiteral(BigDecimal value) {
    try {
      return createLiteral(value.longValueExact());
    } catch (ArithmeticException e) {
      return createDictionaryLiteral(value.toPlainString(), XSD.DECIMAL);
    }
  }

  Literal createLiteral(Literal literal) {
    if (literal instanceof NumericValue) {
      return literal;
    }
    return literal.getLanguage()
            .map(language -> createLiteral(literal.getLabel(), language))
            .orElseGet(() -> createLiteral(literal.getLabel(), literal.getDatatype()));
  }

  @Override
  public void close() {
    if (stringStore != null) {
      stringStore.close();
    }
  }

  interface NumericValue extends Value {
    long encode();
  }

  static final class ItemIRI implements IRI, NumericValue {
    private final long numericId;

    private ItemIRI(long numericId) {
      this.numericId = numericId;
    }

    @Override
    public String getNamespace() {
      return Vocabulary.WD_NAMESPACE;
    }

    @Override
    public String getLocalName() {
      return "Q" + numericId;
    }

    @Override
    public String stringValue() {
      return getNamespace() + getLocalName();
    }

    @Override
    public String toString() {
      return getNamespace() + getLocalName();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o instanceof ItemIRI) {
        return ((ItemIRI) o).numericId == numericId;
      } else {
        return o instanceof IRI && ((IRI) o).stringValue().equals(stringValue());
      }
    }

    @Override
    public int hashCode() {
      return Long.hashCode(numericId);
    }

    @Override
    public long encode() {
      return compose(numericId, TYPE_SHIFT, ITEM_ID_TYPE);
    }
  }

  static final class PropertyIRI implements IRI, NumericValue {
    private final long numericId;
    private final PropertyType propertyType;

    private PropertyIRI(long numericId, PropertyType propertyType) {
      this.numericId = numericId;
      this.propertyType = propertyType;
    }

    private PropertyIRI(long value) {
      this(value / PROPERTY_TYPE_SHIFT, PROPERTY_TYPES[(int) Math.abs(value % PROPERTY_TYPE_SHIFT)]);
    }

    @Override
    public String getNamespace() {
      switch (propertyType) {
        case ENTITY:
          return Vocabulary.WD_NAMESPACE;
        case PROP_DIRECT:
          return Vocabulary.WDT_NAMESPACE;
        case PROP:
          return Vocabulary.P_NAMESPACE;
        case PROP_NOVALUE:
          return Vocabulary.WDNO_NAMESPACE;
        case PROP_STATEMENT:
          return Vocabulary.PS_NAMESPACE;
        case PROP_STATEMENT_VALUE:
          return Vocabulary.PSV_NAMESPACE;
        case PROP_QUALIFIER:
          return Vocabulary.PQ_NAMESPACE;
        case PROP_QUALIFIER_VALUE:
          return Vocabulary.PQV_NAMESPACE;
        case PROP_REFERENCE:
          return Vocabulary.PR_NAMESPACE;
        case PROP_REFERENCE_VALUE:
          return Vocabulary.PRV_NAMESPACE;
        default:
          throw new IllegalStateException("Not supported property type: " + propertyType);
      }
    }

    @Override
    public String getLocalName() {
      return "P" + numericId;
    }

    @Override
    public String stringValue() {
      return getNamespace() + getLocalName();
    }

    @Override
    public String toString() {
      return getNamespace() + getLocalName();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o instanceof PropertyIRI) {
        return ((PropertyIRI) o).numericId == numericId && ((PropertyIRI) o).propertyType == propertyType;
      } else {
        return o instanceof IRI && ((IRI) o).stringValue().equals(stringValue());
      }
    }

    @Override
    public int hashCode() {
      return Long.hashCode(numericId);
    }

    @Override
    public long encode() {
      return compose(compose(numericId, PROPERTY_TYPE_SHIFT, propertyType.ordinal()), TYPE_SHIFT, PROPERTY_ID_TYPE);
    }
  }

  enum PropertyType {
    ENTITY,
    PROP_DIRECT,
    PROP,
    PROP_NOVALUE,
    PROP_STATEMENT,
    PROP_STATEMENT_VALUE,
    PROP_QUALIFIER,
    PROP_QUALIFIER_VALUE,
    PROP_REFERENCE,
    PROP_REFERENCE_VALUE
  }

  private static final PropertyType[] PROPERTY_TYPES = PropertyType.values();

  RevisionIRI createRevisionIRI(long value) {
    return new RevisionIRI(value, Vocabulary.SnapshotType.NONE);
  }

  RevisionIRI createRevisionIRI(long value, Vocabulary.SnapshotType snapshotType) {
    return new RevisionIRI(value, snapshotType);
  }

  static final class RevisionIRI implements IRI, NumericValue {
    private final long revisionId;
    private final Vocabulary.SnapshotType snapshotType;

    private RevisionIRI(long revisionId, Vocabulary.SnapshotType snapshotType) {
      this.revisionId = revisionId;
      this.snapshotType = snapshotType;
    }

    private RevisionIRI(long value) {
      this(value / SNAPSHOT_TYPE_SHIFT, SNAPSHOT_TYPES[(int) Math.abs(value % SNAPSHOT_TYPE_SHIFT)]);
    }

    long getRevisionId() {
      return revisionId;
    }

    Vocabulary.SnapshotType getSnapshotType() {
      return snapshotType;
    }

    RevisionIRI withSnapshotType(Vocabulary.SnapshotType snapshotType) {
      return new RevisionIRI(revisionId, snapshotType);
    }

    RevisionIRI previousRevision() {
      return new RevisionIRI(revisionId - 1, snapshotType);
    }

    RevisionIRI nextRevision() {
      return new RevisionIRI(revisionId + 1, snapshotType);
    }

    @Override
    public String getNamespace() {
      switch (snapshotType) {
        case NONE:
          return Vocabulary.REVISION_NAMESPACE;
        case GLOBAL_STATE:
          return Vocabulary.REVISION_GLOBAL_STATE_NAMESPACE;
        case ADDITIONS:
          return Vocabulary.REVISION_ADDITIONS_NAMESPACE;
        case DELETIONS:
          return Vocabulary.REVISION_DELETIONS_NAMESPACE;
        default:
          throw new IllegalArgumentException("Unknown snapshot type:" + snapshotType);
      }
    }

    @Override
    public String getLocalName() {
      return Long.toString(revisionId);
    }

    @Override
    public String stringValue() {
      return getNamespace() + getLocalName();
    }

    @Override
    public String toString() {
      return getNamespace() + getLocalName();
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      } else if (o instanceof RevisionIRI) {
        return ((RevisionIRI) o).revisionId == revisionId && ((RevisionIRI) o).snapshotType == snapshotType;
      } else {
        return o instanceof IRI && ((IRI) o).stringValue().equals(stringValue());
      }
    }

    @Override
    public int hashCode() {
      return Long.hashCode(revisionId);
    }

    @Override
    public long encode() {
      return compose(compose(revisionId, SNAPSHOT_TYPE_SHIFT, snapshotType.ordinal()), TYPE_SHIFT, REVISION_ID_TYPE);
    }
  }

  private static final Vocabulary.SnapshotType[] SNAPSHOT_TYPES = Vocabulary.SnapshotType.values();

  private static final class DictionaryIRI implements IRI, NumericValue {
    private final long id;
    private final StringStore stringStore;
    private String iri = null;
    private int localNameIdx = -1;

    private DictionaryIRI(long id, StringStore stringStore) {
      this.id = id;
      this.stringStore = stringStore;
    }

    private DictionaryIRI(long id, String iri) {
      this.id = id;
      this.stringStore = null;
      this.iri = iri;
    }

    @Override
    public String stringValue() {
      if (iri == null && stringStore != null) {
        iri = stringStore.getString(id);
      }
      return iri;
    }

    @Override
    public String getNamespace() {
      String iri = stringValue();
      if (localNameIdx < 0) {
        localNameIdx = URIUtil.getLocalNameIndex(iri);
      }
      return iri.substring(0, localNameIdx);
    }

    @Override
    public String getLocalName() {
      String iri = stringValue();
      if (localNameIdx < 0) {
        localNameIdx = URIUtil.getLocalNameIndex(iri);
      }
      return iri.substring(localNameIdx);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof DictionaryIRI) {
        DictionaryIRI other = (DictionaryIRI) obj;
        return id == other.id;
      } else if (obj instanceof IRI) {
        return ((IRI) obj).stringValue().equals(stringValue());
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Long.hashCode(id);
    }

    @Override
    public String toString() {
      return stringValue();
    }

    @Override
    public long encode() {
      return compose(id, TYPE_SHIFT, IRI_TYPE);
    }
  }

  static final class DictionaryBNode implements BNode, NumericValue {
    private final long id;
    private final StringStore stringStore;
    private String nodeID = null;

    private DictionaryBNode(long id, StringStore stringStore) {
      this.id = id;
      this.stringStore = stringStore;
    }

    private DictionaryBNode(long id, String bnodeId) {
      this.id = id;
      this.stringStore = null;
      this.nodeID = bnodeId;
    }

    @Override
    public String stringValue() {
      return getID();
    }

    @Override
    public String getID() {
      if (nodeID == null && stringStore != null) {
        nodeID = stringStore.getString(id);
      }
      return nodeID;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof DictionaryBNode) {
        DictionaryBNode other = (DictionaryBNode) obj;
        return id == other.id;
      } else if (obj instanceof BNode) {
        return ((BNode) obj).getID().equals(getID());
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Long.hashCode(id);
    }

    @Override
    public String toString() {
      return "_:" + getID();
    }

    @Override
    public long encode() {
      return compose(id, TYPE_SHIFT, BLANK_NODE_TYPE);
    }
  }

  private static abstract class NumericLiteral implements Literal, NumericValue {
    @Override
    public Optional<String> getLanguage() {
      return Optional.empty();
    }

    @Override
    public boolean booleanValue() {
      return XMLDatatypeUtil.parseBoolean(getLabel());
    }

    @Override
    public byte byteValue() {
      return XMLDatatypeUtil.parseByte(getLabel());
    }

    @Override
    public short shortValue() {
      return XMLDatatypeUtil.parseShort(getLabel());
    }

    @Override
    public int intValue() {
      return XMLDatatypeUtil.parseInt(getLabel());
    }

    @Override
    public long longValue() {
      return XMLDatatypeUtil.parseLong(getLabel());
    }

    @Override
    public float floatValue() {
      return XMLDatatypeUtil.parseFloat(getLabel());
    }

    @Override
    public double doubleValue() {
      return XMLDatatypeUtil.parseDouble(getLabel());
    }

    @Override
    public BigInteger integerValue() {
      return XMLDatatypeUtil.parseInteger(getLabel());
    }

    @Override
    public BigDecimal decimalValue() {
      return XMLDatatypeUtil.parseDecimal(getLabel());
    }

    @Override
    public XMLGregorianCalendar calendarValue() {
      return XMLDatatypeUtil.parseCalendar(getLabel());
    }

    @Override
    public String stringValue() {
      return getLabel();
    }

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract int hashCode();

    @Override
    public String toString() {
      String label = getLabel();
      IRI datatype = getDatatype();
      StringBuilder sb = new StringBuilder(label.length() + 2);
      sb.append('"').append(label).append('"');
      getLanguage().ifPresent(lang -> sb.append('@').append(lang));
      if (datatype != null && !datatype.equals(XSD.STRING) && !datatype.equals(RDF.LANGSTRING)) {
        sb.append("^^<").append(datatype).append(">");
      }
      return sb.toString();
    }
  }

  static final class TimeLiteral extends NumericLiteral {

    private final long timestamp;

    private TimeLiteral(long timestamp) {
      this.timestamp = timestamp;
    }

    @Override
    public String getLabel() {
      long value = timestamp;
      long seconds = Math.abs(value % 62);
      value /= 62;
      long minutes = Math.abs(value % 62);
      value /= 62;
      long hours = Math.abs(value % 25);
      value /= 25;
      long day = Math.abs(value % 32);
      value /= 32;
      long month = Math.abs(value % 13);
      long year = value / 13;
      return String.format("%04d-%02d-%02dT%02d:%02d:%02dZ", year, month, day, hours, minutes, seconds);
    }

    @Override
    public IRI getDatatype() {
      return XSD.DATETIME;
    }

    @Override
    public XMLGregorianCalendar calendarValue() {
      long value = timestamp;
      int seconds = (int) Math.abs(value % 62);
      value /= 62;
      int minutes = (int) Math.abs(value % 62);
      value /= 62;
      int hours = (int) Math.abs(value % 25);
      value /= 25;
      int day = (int) Math.abs(value % 32);
      value /= 32;
      int month = (int) Math.abs(value % 13);
      BigInteger year = BigInteger.valueOf(value / 13);
      return DATATYPE_FACTORY.newXMLGregorianCalendar(year, month, day, hours, minutes, seconds, BigDecimal.ZERO, 0);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof TimeLiteral) {
        return timestamp == ((TimeLiteral) obj).timestamp;
      } else if (obj instanceof Literal) {
        return XSD.DATETIME.equals(((Literal) obj).getDatatype()) && ((Literal) obj).getLabel().equals(getLabel());
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Long.hashCode(timestamp);
    }

    @Override
    public long encode() {
      return compose(timestamp, TYPE_SHIFT, TIME_TYPE);
    }
  }

  private static final class SmallStringLiteral extends NumericLiteral {
    private final long encoding;

    private SmallStringLiteral(long encoding) {
      this.encoding = encoding;
    }

    @Override
    public String getLabel() {
      return new String(longToBytes(encoding));
    }

    private static byte[] longToBytes(long l) {
      //Compute string len
      int len = 0;
      for (long l2 = l; l2 != 0; l2 >>= 8) {
        len++;
      }
      byte[] result = new byte[len];
      for (int i = 0; i < len; i++) {
        result[i] = (byte) (l & 0xFF);
        l >>= 8;
      }
      return result;
    }

    @Override
    public IRI getDatatype() {
      return XSD.STRING;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof SmallStringLiteral) {
        SmallStringLiteral other = (SmallStringLiteral) obj;
        return encoding == other.encoding;
      } else if (obj instanceof Literal) {
        return XSD.STRING.equals(((Literal) obj).getDatatype()) && ((Literal) obj).getLabel().equals(getLabel());
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Long.hashCode(encoding);
    }

    @Override
    public long encode() {
      return compose(encoding, TYPE_SHIFT, SMALL_STRING_TYPE);
    }
  }

  private static final class DictionaryLiteral extends NumericLiteral {
    private final long id;
    private final short datatypeId;
    private final StringStore stringStore;
    private String label = null;

    private DictionaryLiteral(long id, short datatypeId, StringStore stringStore) {
      this.id = id;
      this.datatypeId = datatypeId;
      this.stringStore = stringStore;
    }

    private DictionaryLiteral(long id, short datatypeId, String label) {
      this.id = id;
      this.datatypeId = datatypeId;
      this.stringStore = null;
      this.label = label;
    }

    private DictionaryLiteral(long id, StringStore stringStore) {
      this(id / DATATYPE_SHIFT, (short) Math.abs(id % DATATYPE_SHIFT), stringStore);
    }

    @Override
    public String getLabel() {
      if (label == null && stringStore != null) {
        label = stringStore.getString(id);
      }
      return label;
    }

    @Override
    public IRI getDatatype() {
      return DATATYPES[datatypeId];
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof DictionaryLiteral) {
        DictionaryLiteral other = (DictionaryLiteral) obj;
        return id == other.id && datatypeId == other.datatypeId;
      } else if (obj instanceof Literal) {
        return ((Literal) obj).getLabel().equals(getLabel()) && ((Literal) obj).getDatatype().equals(getDatatype());
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Long.hashCode(id);
    }

    @Override
    public long encode() {
      return compose(compose(id, DATATYPE_SHIFT, datatypeId), TYPE_SHIFT, LITERAL_TYPE);
    }
  }

  private static final class DictionaryLanguageTaggedString extends NumericLiteral {
    private final long labelId;
    private final short languageId;
    private final StringStore stringStore;
    private String label = null;
    private String language = null;

    private DictionaryLanguageTaggedString(long labelId, short languageId, StringStore stringStore) {
      this.labelId = labelId;
      this.languageId = languageId;
      this.stringStore = stringStore;
    }

    private DictionaryLanguageTaggedString(long labelId, short languageId, String label) {
      this.labelId = labelId;
      this.languageId = languageId;
      this.stringStore = null;
      this.label = label;
    }

    private DictionaryLanguageTaggedString(long id, StringStore stringStore) {
      this(id / LANGUAGE_TAG_SHIFT, (short) Math.abs(id % LANGUAGE_TAG_SHIFT), stringStore);
    }

    @Override
    public String getLabel() {
      if (label == null && stringStore != null) {
        label = stringStore.getString(labelId);
      }
      return label;
    }

    @Override
    public Optional<String> getLanguage() {
      if (language == null && stringStore != null) {
        language = stringStore.getLanguage(languageId);
      }
      return Optional.ofNullable(language);
    }

    @Override
    public IRI getDatatype() {
      return RDF.LANGSTRING;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof DictionaryLanguageTaggedString) {
        DictionaryLanguageTaggedString other = (DictionaryLanguageTaggedString) obj;
        return labelId == other.labelId && languageId == other.languageId;
      } else if (obj instanceof Literal) {
        return RDF.LANGSTRING.equals(((Literal) obj).getDatatype()) && ((Literal) obj).getLabel().equals(getLabel()) && ((Literal) obj).getLanguage().equals(getLanguage());
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Long.hashCode(labelId);
    }

    @Override
    public long encode() {
      return compose(compose(labelId, LANGUAGE_TAG_SHIFT, languageId), TYPE_SHIFT, LANGUAGE_STRING_TYPE);
    }
  }

  private static final class SmallLongDecimalLiteral extends NumericLiteral {
    private final long value;

    private SmallLongDecimalLiteral(long value) {
      this.value = value;
    }

    @Override
    public String getLabel() {
      return Long.toString(value);
    }

    @Override
    public IRI getDatatype() {
      return XSD.DECIMAL;
    }

    @Override
    public long longValue() {
      return value;
    }

    @Override
    public BigInteger integerValue() {
      return BigInteger.valueOf(value);
    }

    @Override
    public BigDecimal decimalValue() {
      return BigDecimal.valueOf(value);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof SmallLongDecimalLiteral) {
        SmallLongDecimalLiteral other = (SmallLongDecimalLiteral) obj;
        return value == other.value;
      } else if (obj instanceof Literal) {
        return XSD.DECIMAL.equals(((Literal) obj).getDatatype()) && ((Literal) obj).getLabel().equals(getLabel());
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Long.hashCode(value);
    }

    @Override
    public long encode() {
      return compose(value, TYPE_SHIFT, SMALL_LONG_DECIMAL_TYPE);
    }
  }

  private static final class SmallLongIntegerLiteral extends NumericLiteral {
    private final long value;

    private SmallLongIntegerLiteral(long value) {
      this.value = value;
    }

    @Override
    public String getLabel() {
      return Long.toString(value);
    }

    @Override
    public IRI getDatatype() {
      return XSD.INTEGER;
    }

    @Override
    public long longValue() {
      return value;
    }

    @Override
    public BigInteger integerValue() {
      return BigInteger.valueOf(value);
    }

    @Override
    public BigDecimal decimalValue() {
      return BigDecimal.valueOf(value);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof SmallLongIntegerLiteral) {
        SmallLongIntegerLiteral other = (SmallLongIntegerLiteral) obj;
        return value == other.value;
      } else if (obj instanceof Literal) {
        return XSD.DECIMAL.equals(((Literal) obj).getDatatype()) && ((Literal) obj).getLabel().equals(getLabel());
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Long.hashCode(value);
    }

    @Override
    public long encode() {
      return compose(value, TYPE_SHIFT, SMALL_LONG_INTEGER_TYPE);
    }
  }

  @Override
  public Statement createStatement(Resource subject, IRI predicate, Value object) {
    return super.createStatement(createResource(subject), createIRI(predicate), createValue(object));
  }

  @Override
  public Statement createStatement(Resource subject, IRI predicate, Value object, Resource context) {
    return super.createStatement(createResource(subject), createIRI(predicate), createValue(object), createResource(context));
  }

  interface StringStore extends AutoCloseable {
    String getString(long id);

    Long putString(String str);

    String getLanguage(short id);

    Short putLanguage(String languageCode);

    @Override
    void close();
  }

  static class EmptyStringStore implements StringStore {
    @Override
    public String getString(long id) {
      return null;
    }

    @Override
    public Long putString(String str) {
      return null;
    }

    @Override
    public String getLanguage(short id) {
      return null;
    }

    @Override
    public Short putLanguage(String languageCode) {
      return null;
    }

    @Override
    public void close() {
    }
  }

  private static long compose(long encoding, long shift, long additional) {
    if (encoding >= 0) {
      return encoding * shift + additional;
    } else {
      return encoding * shift - additional;
    }
  }
}
