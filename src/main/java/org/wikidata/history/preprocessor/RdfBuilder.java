package org.wikidata.history.preprocessor;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.wdtk.datamodel.interfaces.Statement;
import org.wikidata.wdtk.datamodel.interfaces.Value;
import org.wikidata.wdtk.datamodel.interfaces.*;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.Collection;

class RdfBuilder {
  private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();
  private static final Logger LOGGER = LoggerFactory.getLogger(RdfBuilder.class);

  private static final String SCHEMA_PREFIX = "http://schema.org/";
  private static final IRI SCHEMA_ABOUT = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "about");
  private static final IRI SCHEMA_ARTICLE = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "Article");
  private static final IRI SCHEMA_DESCRIPTION = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "description");
  private static final IRI SCHEMA_IN_LANGUAGE = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "inLanguage");
  private static final IRI SCHEMA_IS_PART_OF = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "isPartOf");
  private static final IRI SCHEMA_NAME = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "name");

  private static final String PROV_PREFIX = "http://www.w3.org/ns/prov#";
  private static final IRI PROV_WAS_DERIVED_FROM = VALUE_FACTORY.createIRI(PROV_PREFIX, "wasDerivedFrom");

  private static final String WIKIBASE_PREFIX = "http://wikiba.se/ontology#";
  private static final IRI WIKIBASE_WIKI_GROUP = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "wikiGroup");
  private static final IRI WIKIBASE_BADGE = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "badge");
  private static final IRI WIKIBASE_TIME_VALUE_CLASS = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "TimeValue");
  private static final IRI WIKIBASE_TIME_VALUE = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "timeValue");
  private static final IRI WIKIBASE_TIME_PRECISION = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "timePrecision");
  private static final IRI WIKIBASE_TIME_TIMEZONE = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "timeTimezone");
  private static final IRI WIKIBASE_TIME_CALENDAR_MODEL = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "timeCalendarModel");
  private static final IRI WIKIBASE_GLOBECOORDINATE_VALUE = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "GlobecoordinateValue");
  private static final IRI WIKIBASE_GEO_LATITUDE = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "geoLatitude");
  private static final IRI WIKIBASE_GEO_LONGITUDE = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "geoLongitude");
  private static final IRI WIKIBASE_GEO_PRECISION = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "geoPrecision");
  private static final IRI WIKIBASE_GEO_GLOBE = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "geoGlobe");
  private static final IRI WIKIBASE_QUANTITY_VALUE = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "QuantityValue");
  private static final IRI WIKIBASE_QUANTITY_AMOUNT = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "quantityAmount");
  private static final IRI WIKIBASE_QUANTITY_UPPER_BOUND = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "quantityUpperBound");
  private static final IRI WIKIBASE_QUANTITY_LOWER_BOUND = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "quantityLowerBound");
  private static final IRI WIKIBASE_QUANTITY_UNIT = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "quantityUnit");
  private static final IRI WIKIBASE_STATEMENT = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "Statement");
  private static final IRI WIKIBASE_BEST_RANK = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "BestRank");
  private static final IRI WIKIBASE_RANK = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "rank");
  private static final IRI WIKIBASE_PREFERRED_RANK = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "PreferredRank");
  private static final IRI WIKIBASE_NORMAL_RANK = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "NormalRank");
  private static final IRI WIKIBASE_DEPRECATED_RANK = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "DeprecatedRank");
  private static final IRI WIKIBASE_REFERENCE_CLASS = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "Reference");
  private static final IRI WIKIBASE_PROPERTY_TYPE = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "propertyType");
  private static final IRI WIKIBASE_DIRECT_CLAIM = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "directClaim");
  private static final IRI WIKIBASE_CLAIM = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "claim");
  private static final IRI WIKIBASE_STATEMENT_PROPERTY = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "statementProperty");
  private static final IRI WIKIBASE_STATEMENT_VALUE = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "statementValue");
  private static final IRI WIKIBASE_QUALIFIER = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "qualifier");
  private static final IRI WIKIBASE_QUALIFIER_VALUE = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "qualifierValue");
  private static final IRI WIKIBASE_REFERENCE = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "reference");
  private static final IRI WIKIBASE_REFERENCE_VALUE = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "referenceValue");
  private static final IRI WIKIBASE_NOVALUE = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "novalue");
  private static final IRI WIKIBASE_ITEM = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "Item");
  private static final IRI WIKIBASE_PROPERTY = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "Property");

  private static final String WDS_PREFIX = "http://www.wikidata.org/entity/statement/";
  private static final String WDREF_PREFIX = "http://www.wikidata.org/reference/";

  private static final String WDT_PREFIX = "http://www.wikidata.org/prop/direct/";
  private static final String WDTN_PREFIX = "http://www.wikidata.org/prop/direct-normalized/";
  private static final String P_PREFIX = "http://www.wikidata.org/prop/";
  private static final String WDV_PREFIX = "http://www.wikidata.org/value/";
  private static final String PS_PREFIX = "http://www.wikidata.org/prop/statement/";
  private static final String PSV_PREFIX = "http://www.wikidata.org/prop/statement/value/";
  private static final String PSN_PREFIX = "http://www.wikidata.org/prop/statement/value-normalized/";
  private static final String PQ_PREFIX = "http://www.wikidata.org/prop/qualifier/";
  private static final String PQV_PREFIX = "http://www.wikidata.org/prop/qualifier/value/";
  private static final String PQN_PREFIX = "http://www.wikidata.org/prop/qualifier/value-normalized/";
  private static final String PR_PREFIX = "http://www.wikidata.org/prop/reference/";
  private static final String PRV_PREFIX = "http://www.wikidata.org/prop/reference/value/";
  private static final String PRN_PREFIX = "http://www.wikidata.org/prop/reference/value-normalized/";
  private static final String WDNO_PREFIX = "http://www.wikidata.org/prop/novalue/";
  private static final IRI ONE_ITEM = VALUE_FACTORY.createIRI("http://www.wikidata.org/entity/Q199");

  private final RdfOutput output;
  private final Sites sites;
  private final WikidataPropertyInformation propertyInformation;
  private final WikibaseValueHasher valueHasher = new WikibaseValueHasher();

  RdfBuilder(RdfOutput output, Sites sites, WikidataPropertyInformation propertyInformation) {
    this.output = output;
    this.sites = sites;
    this.propertyInformation = propertyInformation;
  }

  void addEntityDocument(EntityDocument entityDocument) {
    if (entityDocument instanceof ItemDocument) {
      addItemDocument((ItemDocument) entityDocument);
    } else if (entityDocument instanceof PropertyDocument) {
      addPropertyDocument((PropertyDocument) entityDocument);
    }
  }

  private void addItemDocument(ItemDocument itemDocument) {
    IRI subject = convert(itemDocument.getEntityId());
    output.outputStatement(subject, RDF.TYPE, WIKIBASE_ITEM);
    addTermedDocument(subject, itemDocument);
    addStatementDocument(subject, itemDocument);
    for (SiteLink siteLink : itemDocument.getSiteLinks().values()) {
      addSiteLink(subject, siteLink);
    }
  }

  private void addPropertyDocument(PropertyDocument propertyDocument) {
    IRI subject = convert(propertyDocument.getEntityId());
    output.outputStatement(subject, RDF.TYPE, WIKIBASE_PROPERTY);
    addStatementDocument(subject, propertyDocument);
    addTermedDocument(subject, propertyDocument);
    output.outputStatement(subject, WIKIBASE_PROPERTY_TYPE, VALUE_FACTORY.createIRI(propertyDocument.getDatatype().getIri()));
    IRI wdt = VALUE_FACTORY.createIRI(WDT_PREFIX, propertyDocument.getEntityId().getId());
    output.outputStatement(subject, WIKIBASE_DIRECT_CLAIM, wdt);
    IRI p = VALUE_FACTORY.createIRI(P_PREFIX, propertyDocument.getEntityId().getId());
    output.outputStatement(subject, WIKIBASE_CLAIM, p);
    output.outputStatement(p, RDF.TYPE, OWL.OBJECTPROPERTY);
    IRI ps = VALUE_FACTORY.createIRI(PS_PREFIX, propertyDocument.getEntityId().getId());
    output.outputStatement(subject, WIKIBASE_STATEMENT_PROPERTY, ps);
    IRI pq = VALUE_FACTORY.createIRI(PQ_PREFIX, propertyDocument.getEntityId().getId());
    output.outputStatement(subject, WIKIBASE_QUALIFIER, pq);
    IRI pr = VALUE_FACTORY.createIRI(PR_PREFIX, propertyDocument.getEntityId().getId());
    output.outputStatement(subject, WIKIBASE_REFERENCE, pr);
    if (isDatatypePropertyDatatatype(propertyDocument.getDatatype())) {
      output.outputStatement(wdt, RDF.TYPE, OWL.DATATYPEPROPERTY);
      output.outputStatement(ps, RDF.TYPE, OWL.DATATYPEPROPERTY);
      output.outputStatement(pq, RDF.TYPE, OWL.DATATYPEPROPERTY);
      output.outputStatement(pr, RDF.TYPE, OWL.DATATYPEPROPERTY);
    } else {
      output.outputStatement(wdt, RDF.TYPE, OWL.OBJECTPROPERTY);
      output.outputStatement(ps, RDF.TYPE, OWL.OBJECTPROPERTY);
      output.outputStatement(pq, RDF.TYPE, OWL.OBJECTPROPERTY);
      output.outputStatement(pr, RDF.TYPE, OWL.OBJECTPROPERTY);
    }

    if (isComplexValueDatatatype(propertyDocument.getDatatype())) {
      IRI psv = VALUE_FACTORY.createIRI(PSV_PREFIX, propertyDocument.getEntityId().getId());
      output.outputStatement(subject, WIKIBASE_STATEMENT_VALUE, psv);
      output.outputStatement(psv, RDF.TYPE, OWL.OBJECTPROPERTY);
      IRI pqv = VALUE_FACTORY.createIRI(PQV_PREFIX, propertyDocument.getEntityId().getId());
      output.outputStatement(subject, WIKIBASE_QUALIFIER_VALUE, pqv);
      output.outputStatement(pqv, RDF.TYPE, OWL.OBJECTPROPERTY);
      IRI prv = VALUE_FACTORY.createIRI(PRV_PREFIX, propertyDocument.getEntityId().getId());
      output.outputStatement(subject, WIKIBASE_REFERENCE_VALUE, prv);
      output.outputStatement(prv, RDF.TYPE, OWL.OBJECTPROPERTY);
    }

    IRI novalue = VALUE_FACTORY.createIRI(WDNO_PREFIX, propertyDocument.getEntityId().getId());
    BNode novalueComplement = VALUE_FACTORY.createBNode(novalue.stringValue());
    output.outputStatement(subject, WIKIBASE_NOVALUE, novalue);
    output.outputStatement(novalue, RDF.TYPE, OWL.CLASS);
    output.outputStatement(novalue, OWL.COMPLEMENTOF, novalueComplement);
    output.outputStatement(novalueComplement, RDF.TYPE, OWL.RESTRICTION);
    output.outputStatement(novalueComplement, OWL.ONPROPERTY, wdt);
    output.outputStatement(novalueComplement, OWL.SOMEVALUESFROM, OWL.THING);
  }

  private boolean isDatatypePropertyDatatatype(DatatypeIdValue datatypeId) {
    switch (datatypeId.getIri()) {
      case DatatypeIdValue.DT_STRING:
      case DatatypeIdValue.DT_EXTERNAL_ID:
      case DatatypeIdValue.DT_MATH:
      case DatatypeIdValue.DT_GLOBE_COORDINATES:
      case DatatypeIdValue.DT_TIME:
      case DatatypeIdValue.DT_QUANTITY:
        return true;
      case DatatypeIdValue.DT_COMMONS_MEDIA:
      case DatatypeIdValue.DT_ITEM:
      case DatatypeIdValue.DT_PROPERTY:
      case DatatypeIdValue.DT_LEXEME:
      case DatatypeIdValue.DT_FORM:
      case DatatypeIdValue.DT_SENSE:
      case DatatypeIdValue.DT_URL:
      case DatatypeIdValue.DT_GEO_SHAPE:
      case DatatypeIdValue.DT_TABULAR_DATA:
        return false;
      default:
        throw new IllegalArgumentException("Not expected datatype: " + datatypeId);
    }
  }

  private boolean isComplexValueDatatatype(DatatypeIdValue datatypeId) {
    switch (datatypeId.getIri()) {
      case DatatypeIdValue.DT_GLOBE_COORDINATES:
      case DatatypeIdValue.DT_TIME:
      case DatatypeIdValue.DT_QUANTITY:
        return true;
      default:
        return false;
    }
  }

  private void addTermedDocument(IRI subject, TermedDocument termedDocument) {
    for (MonolingualTextValue value : termedDocument.getLabels().values()) {
      output.outputStatement(subject, RDFS.LABEL, convert(value));
    }
    for (MonolingualTextValue value : termedDocument.getDescriptions().values()) {
      output.outputStatement(subject, SCHEMA_DESCRIPTION, convert(value));
    }
    for (Collection<MonolingualTextValue> values : termedDocument.getAliases().values()) {
      for (MonolingualTextValue value : values) {
        output.outputStatement(subject, SKOS.ALT_LABEL, convert(value));
      }
    }
  }

  private void addStatementDocument(IRI subject, StatementDocument statementDocument) {
    for (StatementGroup group : statementDocument.getStatementGroups()) {
      addStatementGroup(subject, group);
    }
  }

  private void addStatementGroup(IRI subject, StatementGroup statements) {
    StatementRank bestRank = getBestRank(statements);
    for (Statement statement : statements) {
      addFullStatement(subject, statement, bestRank);
    }
  }

  private void addFullStatement(IRI subject, Statement statement, StatementRank bestRank) {
    IRI statementIRI = VALUE_FACTORY.createIRI(WDS_PREFIX, statement.getStatementId().replace("$", "-"));
    Snak mainSnak = statement.getMainSnak();
    PropertyIdValue mainProperty = mainSnak.getPropertyId();

    // wdt:
    if (statement.getRank().equals(bestRank)) {
      addSimpleValueSnak(subject, mainSnak, WDT_PREFIX, statementIRI);
    }

    //Input
    output.outputStatement(subject, VALUE_FACTORY.createIRI(P_PREFIX, mainProperty.getId()), statementIRI);

    //Types
    output.outputStatement(statementIRI, RDF.TYPE, WIKIBASE_STATEMENT);
    if (statement.getRank().equals(bestRank)) {
      output.outputStatement(statementIRI, RDF.TYPE, WIKIBASE_BEST_RANK);
    }

    //Main snak
    addSnak(statementIRI, mainSnak, PS_PREFIX, PSV_PREFIX, statementIRI);

    //Rank
    output.outputStatement(statementIRI, WIKIBASE_RANK, convert(statement.getRank()));

    //Qualifiers
    statement.getAllQualifiers().forEachRemaining(snak -> addSnak(statementIRI, snak, PQ_PREFIX, PQV_PREFIX, statementIRI));

    for (Reference reference : statement.getReferences()) {
      IRI referenceIri = VALUE_FACTORY.createIRI(WDREF_PREFIX, valueHasher.hash(reference));
      output.outputStatement(statementIRI, PROV_WAS_DERIVED_FROM, referenceIri);
      output.outputStatement(referenceIri, RDF.TYPE, WIKIBASE_REFERENCE_CLASS);
      reference.getAllSnaks().forEachRemaining(snak -> addSnak(referenceIri, snak, PR_PREFIX, PRV_PREFIX, referenceIri));
    }
  }

  private void addSnak(IRI subject, Snak snak, String simplePrefix, String complexPrefix, IRI contextIRI) {
    addSimpleValueSnak(subject, snak, simplePrefix, contextIRI);
    addComplexValueSnak(subject, snak, complexPrefix);
  }

  private void addSimpleValueSnak(IRI subject, Snak snak, String prefix, IRI contextIRI) {
    IRI propertyIri = VALUE_FACTORY.createIRI(prefix, snak.getPropertyId().getId());
    if (snak instanceof ValueSnak) {
      org.eclipse.rdf4j.model.Value simpleValue = convertSimple(((ValueSnak) snak).getValue(), snak.getPropertyId());
      output.outputStatement(subject, propertyIri, simpleValue);
    } else if (snak instanceof SomeValueSnak) {
      output.outputStatement(subject, propertyIri, VALUE_FACTORY.createBNode(valueHasher.hash(contextIRI, snak.getPropertyId())));
    } else if (snak instanceof NoValueSnak) {
      output.outputStatement(subject, RDF.TYPE, VALUE_FACTORY.createIRI(WDNO_PREFIX, snak.getPropertyId().getId()));
    } else {
      throw new IllegalArgumentException("Unexpected snak type: " + snak);
    }
  }

  private void addComplexValueSnak(IRI subject, Snak snak, String prefix) {
    if (snak instanceof ValueSnak) {
      org.eclipse.rdf4j.model.Value fullValue = convertFull(((ValueSnak) snak).getValue());
      if (fullValue != null) {
        output.outputStatement(subject, VALUE_FACTORY.createIRI(prefix, snak.getPropertyId().getId()), fullValue);
      }
    }
  }

  private void addSiteLink(IRI subject, SiteLink siteLink) {
    String url = sites.getSiteLinkUrl(siteLink);
    if (url == null) {
      LOGGER.warn("The site " + siteLink.getSiteKey() + " is unknown");
      return;
    }
    IRI article = convertIRI(url);
    output.outputStatement(article, RDF.TYPE, SCHEMA_ARTICLE);
    output.outputStatement(article, SCHEMA_ABOUT, subject);
    String languageCode = sites.getLanguageCode(siteLink.getSiteKey());
    if (languageCode != null) {
      languageCode = convertLanguageCode(languageCode);
      output.outputStatement(article, SCHEMA_IN_LANGUAGE, VALUE_FACTORY.createLiteral(languageCode));
      output.outputStatement(article, SCHEMA_NAME, VALUE_FACTORY.createLiteral(siteLink.getPageTitle(), convertLanguageCode(languageCode)));
    }
    IRI wiki = VALUE_FACTORY.createIRI(article.stringValue().split("wiki/")[0]);
    output.outputStatement(article, SCHEMA_IS_PART_OF, wiki);
    for (ItemIdValue badge : siteLink.getBadges()) {
      output.outputStatement(article, WIKIBASE_BADGE, convert(badge));
    }
    output.outputStatement(wiki, WIKIBASE_WIKI_GROUP, VALUE_FACTORY.createLiteral(sites.getGroup(siteLink.getSiteKey())));
  }

  private org.eclipse.rdf4j.model.Value convertSimple(Value value, PropertyIdValue propertyId) {
    if (value instanceof EntityIdValue) {
      return convert(((EntityIdValue) value));
    } else if (value instanceof StringValue) {
      return convert((StringValue) value, propertyId);
    } else if (value instanceof MonolingualTextValue) {
      return convert((MonolingualTextValue) value);
    } else if (value instanceof TimeValue) {
      return convertSimple((TimeValue) value);
    } else if (value instanceof GlobeCoordinatesValue) {
      return convertSimple((GlobeCoordinatesValue) value);
    } else if (value instanceof QuantityValue) {
      return convertSimple((QuantityValue) value);
    } else {
      throw new IllegalArgumentException("Not supported value type: " + value);
    }
  }

  private org.eclipse.rdf4j.model.Value convertFull(Value value) {
    if (value instanceof TimeValue) {
      return convertFull((TimeValue) value);
    } else if (value instanceof GlobeCoordinatesValue) {
      return convertFull((GlobeCoordinatesValue) value);
    } else if (value instanceof QuantityValue) {
      return convertFull((QuantityValue) value);
    } else {
      return null;
    }
  }

  private IRI convert(EntityIdValue value) {
    return VALUE_FACTORY.createIRI(value.getIri());
  }

  private Literal convert(MonolingualTextValue value) {
    return VALUE_FACTORY.createLiteral(value.getText(), convertLanguageCode(value.getLanguageCode()));
  }

  private org.eclipse.rdf4j.model.Value convert(StringValue value, PropertyIdValue propertyId) {
    //TODO: proper escaping
    switch (propertyInformation.getDatatypeIRI(propertyId)) {
      case DatatypeIdValue.DT_COMMONS_MEDIA:
        return convertIRI("http://commons.wikimedia.org/wiki/Special:FilePath/" + value.getString());
      case DatatypeIdValue.DT_URL:
        return convertIRI(value.getString());
      case DatatypeIdValue.DT_GEO_SHAPE:
      case DatatypeIdValue.DT_TABULAR_DATA:
        return convertIRI("http://commons.wikimedia.org/data/main/" + value.getString());
      default:
        return VALUE_FACTORY.createLiteral(value.getString());
    }
  }

  private Literal convertSimple(TimeValue value) {
    return convertDateTime(value);
  }

  private IRI convertFull(TimeValue value) {
    IRI node = VALUE_FACTORY.createIRI(WDV_PREFIX, valueHasher.hash(value));
    output.outputStatement(node, RDF.TYPE, WIKIBASE_TIME_VALUE_CLASS);
    output.outputStatement(node, WIKIBASE_TIME_VALUE, convertDateTime(value));
    output.outputStatement(node, WIKIBASE_TIME_PRECISION, convert((int) value.getPrecision()));
    output.outputStatement(node, WIKIBASE_TIME_TIMEZONE, convert(value.getTimezoneOffset()));
    output.outputStatement(node, WIKIBASE_TIME_CALENDAR_MODEL, convertIRI(value.getPreferredCalendarModel()));
    return node;
  }

  private Literal convertSimple(GlobeCoordinatesValue value) {
    String wkt = "Point(" + value.getLongitude() + " " + value.getLatitude() + ")";
    if (!value.getGlobe().equals(GlobeCoordinatesValue.GLOBE_EARTH)) {
      wkt += " <" + value.getGlobe() + ">";
    }
    return VALUE_FACTORY.createLiteral(wkt, GEO.WKT_LITERAL);
  }

  private IRI convertFull(GlobeCoordinatesValue value) {
    IRI node = VALUE_FACTORY.createIRI(WDV_PREFIX, valueHasher.hash(value));
    output.outputStatement(node, RDF.TYPE, WIKIBASE_GLOBECOORDINATE_VALUE);
    output.outputStatement(node, WIKIBASE_GEO_LATITUDE, VALUE_FACTORY.createLiteral(value.getLatitude()));
    output.outputStatement(node, WIKIBASE_GEO_LONGITUDE, VALUE_FACTORY.createLiteral(value.getLongitude()));
    output.outputStatement(node, WIKIBASE_GEO_PRECISION, VALUE_FACTORY.createLiteral(value.getPrecision()));
    output.outputStatement(node, WIKIBASE_GEO_GLOBE, convertIRI(value.getGlobe()));
    return node;
  }

  private Literal convertSimple(QuantityValue value) {
    return convert(value.getNumericValue());
  }

  private IRI convertFull(QuantityValue value) {
    IRI node = VALUE_FACTORY.createIRI(WDV_PREFIX, valueHasher.hash(value));
    output.outputStatement(node, RDF.TYPE, WIKIBASE_QUANTITY_VALUE);
    output.outputStatement(node, WIKIBASE_QUANTITY_AMOUNT, convert(value.getNumericValue()));
    if (value.getUpperBound() != null) {
      output.outputStatement(node, WIKIBASE_QUANTITY_UPPER_BOUND, convert(value.getUpperBound()));
    }
    if (value.getLowerBound() != null) {
      output.outputStatement(node, WIKIBASE_QUANTITY_LOWER_BOUND, convert(value.getLowerBound()));
    }
    if ("1".equals(value.getUnit())) {
      output.outputStatement(node, WIKIBASE_QUANTITY_UNIT, ONE_ITEM);
    } else {
      output.outputStatement(node, WIKIBASE_QUANTITY_UNIT, convertIRI(value.getUnit()));
    }
    return node;
  }

  private Literal convert(BigDecimal value) {
    if (value.signum() != -1) {
      return VALUE_FACTORY.createLiteral("+" + value.toPlainString(), XMLSchema.DECIMAL);
    } else {
      return VALUE_FACTORY.createLiteral(value.toPlainString(), XMLSchema.DECIMAL);
    }
  }

  private Literal convert(int value) {
    return VALUE_FACTORY.createLiteral(Integer.toString(value), XMLSchema.INTEGER);
  }

  private IRI convert(StatementRank rank) {
    switch (rank) {
      case PREFERRED:
        return WIKIBASE_PREFERRED_RANK;
      case NORMAL:
        return WIKIBASE_NORMAL_RANK;
      case DEPRECATED:
        return WIKIBASE_DEPRECATED_RANK;
      default:
        return null;
    }
  }

  /**
   * Port of Wikibase DateTimeValueCleaner by Stas Malyshev and Thiemo Kreuz (GPLv2+)
   */
  private Literal convertDateTime(TimeValue value) {
    StringBuilder builder = new StringBuilder();
    DecimalFormat yearForm = new DecimalFormat("0000");
    DecimalFormat timeForm = new DecimalFormat("00");
    if (value.getYear() > 0) {
      builder.append("+");
    }
    builder.append(yearForm.format(value.getYear()));
    builder.append("-");
    builder.append(timeForm.format(value.getMonth()));
    builder.append("-");
    builder.append(timeForm.format(value.getDay()));
    builder.append("T");
    builder.append(timeForm.format(value.getHour()));
    builder.append(":");
    builder.append(timeForm.format(value.getMinute()));
    builder.append(":");
    builder.append(timeForm.format(value.getSecond()));
    builder.append("Z");
    String time = builder.toString();
    return VALUE_FACTORY.createLiteral(time, XMLSchema.DATETIME);

    /*if ( !value.getPreferredCalendarModel().equals(TimeValue.CM_GREGORIAN_PRO) && value.getPrecision() >= TimeValue.PREC_DAY ) {
      return VALUE_FACTORY.createLiteral(
              (value.getYear() >= 0 ? "+" : "-") +
                      String.format(
                              "%04d-%02d-%02dT%02d:%02d:%02dZ",
                              value.getYear(), value.getMonth(), value.getDay(), value.getHour(), value.getMinute(), value.getSecond()
                      )
      );
    }

    byte precision = value.getPrecision();
    long y = value.getYear();
    byte m = value.getMonth();
    byte d = value.getDay();
    if (m <= 0) {
      m = 1;
    }
    if (m >= 12) {
      // Why anybody would do something like that? Anyway, better to check.
      m = 12;
    }
    if (d <= 0) {
      d = 1;
    }

    if (y == 0) {
      // Year 0 is invalid for now, see T94064 for discussion
      LOGGER.warn("Time with year 0: " + value);
      return VALUE_FACTORY.createLiteral(
              String.format("+0000-%02d-%02dT%02d:%02d:%02dZ", m, d, value.getHour(), value.getMinute(), value.getSecond())
      );
    }

    if (precision <= TimeValue.PREC_YEAR) {
      // If we don't have day precision, don't bother cleaning up day values
      d = 1;
      m = 1;
    } else if (precision == TimeValue.PREC_MONTH) {
      d = 1;
    }

    // check if the date "looks safe". If not, we do deeper check
    if ( !( d <= 28 || ( m != 2 && d <= 30 ) ) ) {
      // PHP source docs say PHP gregorian calendar can work down to 4714 BC
      int safeYear = (-4713 < y && y < Integer.MAX_VALUE) ? (int) y : -4713;
      // This will convert y to int. If it's not within sane range,
      // Feb 29 may be mangled, but this will be rare.
      byte max = (byte) YearMonth.of(safeYear, (int) m).lengthOfMonth();
      // We just put it as the last day in month, won't bother further
      if ( d > max ) {
        d = max;
      }
    }

    if (precision >= TimeValue.PREC_YEAR && y < 0) {
      // If we have year's or finer precision, to make year match XSD 1.1 we
      // need to bump up the negative years by 1
      // Note that y is an absolute value here.
      y += 1;
    }

    // This is a bit weird since xsd:dateTime requires >=4 digit always,
    // and leading 0 is not allowed for 5 digits, but sprintf counts - as digit
    // See: http://www.w3.org/TR/xmlschema-2/#dateTime
    return VALUE_FACTORY.createLiteral(((y < 0) ? "-" : "") + String.format("%04d-%02d-%02dT%02d:%02d:%02dZ",
            Math.abs(y), m, d, value.getHour(), value.getMinute(), value.getSecond()), XMLSchema.DATETIME);*/
  }

  private String convertLanguageCode(String languageCode) {
    try {
      return WikimediaLanguageCodes.getLanguageCode(languageCode);
    } catch (IllegalArgumentException e) {
      return languageCode;
    }
  }

  private IRI convertIRI(String iri) {
    return VALUE_FACTORY.createIRI(iri.trim()
            .replace(" ", "%20")
            .replace("\"", "%22")
            .replace("<", "%3C")
            .replace(">", "%3E")
            .replace("\\", "%5C")
            .replace("`", "%60")
            .replace("^", "%5E")
            .replace("|", "%7C")
            .replace("{", "%7B")
            .replace("}", "%7D")
    );
  }

  private StatementRank getBestRank(StatementGroup statementGroup) {
    for (Statement statement : statementGroup.getStatements()) {
      if (StatementRank.PREFERRED.equals(statement.getRank())) {
        return StatementRank.PREFERRED;
      }
    }
    return StatementRank.NORMAL;
  }

  public interface RdfOutput {
    default void outputStatement(Resource subject, IRI predicate, org.eclipse.rdf4j.model.Value object) {
      outputStatement(VALUE_FACTORY.createStatement(subject, predicate, object));
    }

    void outputStatement(org.eclipse.rdf4j.model.Statement statement);
  }
}
