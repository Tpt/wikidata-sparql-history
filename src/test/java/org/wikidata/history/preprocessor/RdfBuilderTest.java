package org.wikidata.history.preprocessor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.wikidata.wdtk.datamodel.helpers.Datamodel;
import org.wikidata.wdtk.datamodel.helpers.DatamodelMapper;
import org.wikidata.wdtk.datamodel.implementation.EntityDocumentImpl;
import org.wikidata.wdtk.datamodel.interfaces.EntityDocument;
import org.wikidata.wdtk.datamodel.interfaces.Sites;
import org.wikidata.wdtk.dumpfiles.DumpProcessingController;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class RdfBuilderTest {

  private static final ObjectMapper OBJECT_MAPPER = new DatamodelMapper(Datamodel.SITE_WIKIDATA);
  private static final List<String> HASHED_NAMESPACES = Arrays.asList(
          "http://www.wikidata.org/value/",
          "http://www.wikidata.org/reference/"
  );

  private final Sites sites;
  private final WikidataPropertyInformation PROPERTY_INFORMATION;

  RdfBuilderTest() throws IOException {
    sites = (new DumpProcessingController("wikidatawiki")).getSitesInformation();
    PROPERTY_INFORMATION = new WikidataPropertyInformation();
  }

  @Test
  void testItemConversion() throws IOException {
    testEntityConversion("Q3");
    testEntityConversion("Q4");
    testEntityConversion("Q6");
    testEntityConversion("Q7");
    //TODO: date normalization testEntityConversion("Q8");
  }

  @Test
  void testPropertyConversion() throws IOException {
    testEntityConversion("P2");
    testEntityConversion("P3");
  }

  private void testEntityConversion(String entityId) throws IOException {
    EntityDocument entity = OBJECT_MAPPER.readValue(getClass().getResource("/entities/" + entityId + ".json"), EntityDocumentImpl.class);
    Model expected = makeHashedBlankNodes(Rio.parse(getClass().getResourceAsStream("/rdf/" + entityId + ".nt"), "", RDFFormat.NTRIPLES));
    ModelRdfOutput output = new ModelRdfOutput();
    RdfBuilder rdfBuilder = new RdfBuilder(output, sites, PROPERTY_INFORMATION);
    rdfBuilder.addEntityDocument(entity);
    Model actual = makeHashedBlankNodes(output.getModel());
    if (!Models.isomorphic(expected, actual)) {
      Assertions.fail("Mapping failed." + diff(expected, actual));
    }
  }

  private Model makeHashedBlankNodes(Model model) {
    Model newModel = new LinkedHashModel(model.size());
    model.forEach(statement -> newModel.add(
            (Resource) makeHashedBlankNodes(statement.getSubject()),
            (IRI) makeHashedBlankNodes(statement.getPredicate()),
            makeHashedBlankNodes(statement.getObject())
    ));
    return newModel;
  }

  private Value makeHashedBlankNodes(Value value) {
    if (value instanceof IRI) {
      IRI iri = (IRI) value;
      if (HASHED_NAMESPACES.contains(iri.getNamespace())) {
        return SimpleValueFactory.getInstance().createBNode(iri.stringValue());
      }
    }
    return value;
  }

  private String diff(Model expected, Model actual) {
    Model missing = new LinkedHashModel();
    Model extra = new LinkedHashModel();
    for (Statement statement : expected) {
      if (notInWithoutBNode(actual, statement)) {
        missing.add(statement);
      }
    }
    for (Statement statement : actual) {
      if (notInWithoutBNode(expected, statement)) {
        extra.add(statement);
      }
    }
    return "\nMissing:\n" + toNt(missing) + "\nExtra:\n" + toNt(extra);
  }

  private boolean notInWithoutBNode(Model model, Statement statement) {
    return model.filter(
            statement.getSubject() instanceof BNode ? null : statement.getSubject(),
            statement.getPredicate(),
            statement.getObject() instanceof BNode ? null : statement.getObject()
    ).isEmpty();
  }

  private String toNt(Model model) {
    return model.stream().map(statement -> NTriplesUtil.toNTriplesString(statement.getSubject()) + " " +
            NTriplesUtil.toNTriplesString(statement.getPredicate()) + " " +
            NTriplesUtil.toNTriplesString(statement.getObject()) + " . "
    ).sorted().collect(Collectors.joining("\n"));
  }

  private static final class ModelRdfOutput implements RdfBuilder.RdfOutput {
    private final Model model = new LinkedHashModel();

    @Override
    public void outputStatement(Statement statement) {
      model.add(statement);
    }

    Model getModel() {
      return model;
    }
  }
}
