package org.wikidata.history.preprocessor;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

import java.io.IOException;
import java.time.Instant;

interface HistoryOutput extends AutoCloseable {
  void addRevision(
          long revisionId, long parentRevisionId, String entityId, Instant timestamp, String contributorName, String comment
  ) throws IOException;

  void addTriple(Resource subject, IRI predicate, Value object, long... revisionIds) throws IOException;
}
