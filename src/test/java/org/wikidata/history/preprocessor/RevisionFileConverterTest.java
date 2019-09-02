package org.wikidata.history.preprocessor;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.io.IOUtils;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.junit.Test;
import org.wikidata.history.sparql.Vocabulary;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class RevisionFileConverterTest {

  private static final Map<Statement, long[]> EXPECTED_TRIPLES = new HashMap<>();

  static {
    ValueFactory vf = SimpleValueFactory.getInstance();
    EXPECTED_TRIPLES.put(vf.createStatement(
            vf.createIRI(Vocabulary.WD_NAMESPACE, "Q1"),
            RDF.TYPE,
            vf.createIRI(Vocabulary.WB_NAMESPACE, "Item")
    ), new long[]{2, Long.MAX_VALUE});
    EXPECTED_TRIPLES.put(vf.createStatement(
            vf.createIRI(Vocabulary.WD_NAMESPACE, "Q1"),
            RDFS.LABEL,
            vf.createLiteral("foo", "fr")
    ), new long[]{2, Long.MAX_VALUE});
    EXPECTED_TRIPLES.put(vf.createStatement(
            vf.createIRI(Vocabulary.WD_NAMESPACE, "Q1"),
            RDFS.LABEL,
            vf.createLiteral("bar", "en")
    ), new long[]{2, 9});
    EXPECTED_TRIPLES.put(vf.createStatement(
            vf.createIRI(Vocabulary.WD_NAMESPACE, "Q1"),
            RDFS.LABEL,
            vf.createLiteral("foo", "en")
    ), new long[]{9, Long.MAX_VALUE});
    EXPECTED_TRIPLES.put(vf.createStatement(
            vf.createIRI(Vocabulary.WD_NAMESPACE, "Q1"),
            RDFS.LABEL,
            vf.createLiteral("bar", "de")
    ), new long[]{2, 9, 11, Long.MAX_VALUE});
    EXPECTED_TRIPLES.put(vf.createStatement(
            vf.createIRI(Vocabulary.WD_NAMESPACE, "Q1"),
            RDFS.LABEL,
            vf.createLiteral("foo", "es")
    ), new long[]{9, Long.MAX_VALUE});
  }

  @Test
  public void test() throws IOException, InterruptedException {
    ListHistoryOutput output = new ListHistoryOutput();
    RevisionFileConverter revisionFileConverter = new RevisionFileConverter(output);
    revisionFileConverter.process(makeDumpFile());
    assertMapEquals(EXPECTED_TRIPLES, output.triples);
  }

  private Path makeDumpFile() throws IOException {
    Path file = Files.createTempFile("foo", ".xml.bz2");
    try (
            InputStream input = getClass().getResourceAsStream("/dump_file_sample.xml");
            OutputStream output = new BZip2CompressorOutputStream(Files.newOutputStream(file))
    ) {
      IOUtils.copy(input, output);

    }
    return file;
  }

  private static final class ListHistoryOutput implements HistoryOutput {

    private final Map<Statement, long[]> triples = new HashMap<>();

    @Override
    public void addRevision(long revisionId, long parentRevisionId, String entityId, Instant timestamp, String contributorName, String comment) {
    }

    @Override
    public void addTriple(Resource subject, IRI predicate, Value object, long... revisionIds) {
      triples.put(SimpleValueFactory.getInstance().createStatement(subject, predicate, object), revisionIds);
    }

    @Override
    public void close() {

    }
  }

  private static <K, V> void assertMapEquals(Map<K, V> expected, Map<K, V> actual) {
    assertEquals(expected.size(), actual.size());
    for (Map.Entry<K, V> e : expected.entrySet()) {
      if (e.getValue() instanceof long[]) {
        assertArrayEquals((long[]) e.getValue(), (long[]) actual.get(e.getKey()));
      } else {
        assertEquals(e.getValue(), actual.get(e.getKey()));
      }
    }
  }
}
