package org.wikidata.history.preprocessor;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.AbstractTupleQueryResultHandler;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.wikidata.wdtk.datamodel.helpers.Datamodel;
import org.wikidata.wdtk.datamodel.interfaces.DatatypeIdValue;
import org.wikidata.wdtk.datamodel.interfaces.PropertyIdValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class WikidataPropertyInformation {

  private static final String WDQS_ENDPOINT = "https://query.wikidata.org/sparql";
  private static final String USER_AGENT = "WikidataHistoryLoader/0.1";
  private static final String QUERY = "SELECT ?property ?datatype ?uriPattern WHERE { ?property wikibase:propertyType ?datatype . OPTIONAL { ?property wdt:P1921 ?uriPattern }}";

  private final Map<PropertyIdValue, DatatypeIdValue> datatypes = new HashMap<>();
  private final Map<PropertyIdValue, String> uriPatterns = new HashMap<>();

  WikidataPropertyInformation() {
    SPARQLRepository repository = new SPARQLRepository(WDQS_ENDPOINT);
    repository.setAdditionalHttpHeaders(Collections.singletonMap("User-Agent", USER_AGENT));
    repository.initialize();
    try (RepositoryConnection connection = repository.getConnection()) {
      connection.prepareTupleQuery(QUERY).evaluate(new AbstractTupleQueryResultHandler() {
        @Override
        public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
          IRI propertyIRI = (IRI) bindingSet.getValue("property");
          PropertyIdValue property = Datamodel.makePropertyIdValue(propertyIRI.getLocalName(), propertyIRI.getNamespace());
          datatypes.put(property, Datamodel.makeDatatypeIdValue(bindingSet.getValue("datatype").stringValue()));
          if (bindingSet.hasBinding("uriPattern")) {
            uriPatterns.put(property, bindingSet.getValue("uriPattern").stringValue());
          }
        }
      });
    }
    repository.shutDown();
  }

  DatatypeIdValue getDatatype(PropertyIdValue propertyId) {
    return datatypes.get(propertyId);
  }

  String getDatatypeIRI(PropertyIdValue propertyId) {
    DatatypeIdValue datatype = datatypes.get(propertyId);
    return datatype == null ? DatatypeIdValue.DT_STRING : datatype.getIri();
  }

  String getUriPattern(PropertyIdValue propertyId) {
    return uriPatterns.get(propertyId);
  }
}
