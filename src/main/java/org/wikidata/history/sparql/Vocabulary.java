package org.wikidata.history.sparql;

import com.google.common.collect.Sets;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;

import java.util.Set;

public final class Vocabulary {
  enum SnapshotType {
    NONE,
    GLOBAL_STATE,
    ADDITIONS,
    DELETIONS
  }

  private static final SimpleValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();

  public static final IRI SCHEMA_ABOUT = VALUE_FACTORY.createIRI("http://schema.org/about");
  public static final IRI SCHEMA_AUTHOR = VALUE_FACTORY.createIRI("http://schema.org/author");
  public static final IRI SCHEMA_DATE_CREATED = VALUE_FACTORY.createIRI("http://schema.org/dateCreated");
  public static final IRI SCHEMA_IS_BASED_ON = VALUE_FACTORY.createIRI("http://schema.org/isBasedOn");

  public static final IRI HISTORY_ADDITION = VALUE_FACTORY.createIRI("http://wikiba.se/history/ontology#addition");
  public static final IRI HISTORY_DELETION = VALUE_FACTORY.createIRI("http://wikiba.se/history/ontology#deletion");
  public static final IRI HISTORY_ADDITIONS = VALUE_FACTORY.createIRI("http://wikiba.se/history/ontology#additions");
  public static final IRI HISTORY_DELETIONS = VALUE_FACTORY.createIRI("http://wikiba.se/history/ontology#deletions");
  public static final IRI HISTORY_PREVIOUS_REVISION = VALUE_FACTORY.createIRI("http://wikiba.se/history/ontology#previousRevision");
  public static final IRI HISTORY_NEXT_REVISION = VALUE_FACTORY.createIRI("http://wikiba.se/history/ontology#nextRevision");
  public static final IRI HISTORY_GLOBAL_STATE = VALUE_FACTORY.createIRI("http://wikiba.se/history/ontology#globalState");
  public static final IRI HISTORY_REVISION_ID = VALUE_FACTORY.createIRI("http://wikiba.se/history/ontology#revisionId");

  public static final String WD_NAMESPACE = "http://www.wikidata.org/entity/";
  public static final String WDS_NAMESPACE = "http://www.wikidata.org/entity/statement/";
  public static final String WDV_NAMESPACE = "http://www.wikidata.org/value/";
  public static final String WDREF_NAMESPACE = "http://www.wikidata.org/reference/";
  public static final String WDT_NAMESPACE = "http://www.wikidata.org/prop/direct/";
  public static final String P_NAMESPACE = "http://www.wikidata.org/prop/";
  public static final String WDNO_NAMESPACE = "http://www.wikidata.org/prop/novalue/";
  public static final String PS_NAMESPACE = "http://www.wikidata.org/prop/statement/";
  public static final String PSV_NAMESPACE = "http://www.wikidata.org/prop/statement/value/";
  public static final String PQ_NAMESPACE = "http://www.wikidata.org/prop/qualifier/";
  public static final String PQV_NAMESPACE = "http://www.wikidata.org/prop/qualifier/value/";
  public static final String PR_NAMESPACE = "http://www.wikidata.org/prop/reference/";
  public static final String PRV_NAMESPACE = "http://www.wikidata.org/prop/reference/value/";

  public static final String REVISION_NAMESPACE = "http://www.wikidata.org/revision/";
  public static final String REVISION_ADDITIONS_NAMESPACE = REVISION_NAMESPACE + "additions/";
  public static final String REVISION_DELETIONS_NAMESPACE = REVISION_NAMESPACE + "deletions/";
  public static final String REVISION_GLOBAL_STATE_NAMESPACE = REVISION_NAMESPACE + "global/";

  public static final IRI CURRENT_GLOBAL_STATE = VALUE_FACTORY.createIRI(REVISION_GLOBAL_STATE_NAMESPACE, Long.toString(Long.MAX_VALUE / 256));

  static final IRI P279_CLOSURE = VALUE_FACTORY.createIRI("http://wikiba.se/history/ontology#p279Closure");

  private static final Set<String> REVISION_NAMESPACES = Sets.newHashSet(REVISION_NAMESPACE, REVISION_GLOBAL_STATE_NAMESPACE, REVISION_ADDITIONS_NAMESPACE, REVISION_DELETIONS_NAMESPACE);

  public static IRI toDirectProperty(IRI propertyIri) {
    if (!propertyIri.getNamespace().equals(WD_NAMESPACE) || !propertyIri.getLocalName().startsWith("P")) {
      throw new IllegalArgumentException("Not valid property IRI: " + propertyIri);
    }
    return VALUE_FACTORY.createIRI(WDT_NAMESPACE, propertyIri.getLocalName());
  }

  public static IRI toGlobalState(IRI revisionIRI) {
    assertsInRevisionNamespace(revisionIRI);
    return VALUE_FACTORY.createIRI(Vocabulary.REVISION_GLOBAL_STATE_NAMESPACE, revisionIRI.getLocalName());
  }

  public static IRI previousRevision(IRI revisionIRI) {
    assertsInRevisionNamespace(revisionIRI);
    return VALUE_FACTORY.createIRI(Vocabulary.REVISION_NAMESPACE, Integer.toString(Integer.parseInt(revisionIRI.getLocalName()) - 1));
  }

  private static void assertsInRevisionNamespace(IRI revisionIRI) {
    if (!REVISION_NAMESPACES.contains(revisionIRI.getNamespace())) {
      throw new QueryEvaluationException("Not supported revision IRI: " + revisionIRI);
    }
  }
}
