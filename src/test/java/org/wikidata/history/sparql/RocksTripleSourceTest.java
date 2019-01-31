package org.wikidata.history.sparql;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

public class RocksTripleSourceTest {

  private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();
  private static final List<Statement> STATEMENTS = Arrays.asList(
          VALUE_FACTORY.createStatement(
                  VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"),
                  VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P31"),
                  VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q5"),
                  VALUE_FACTORY.createIRI(Vocabulary.REVISION_NAMESPACE, "42")
          ),
          VALUE_FACTORY.createStatement(
                  VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"),
                  VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P21"),
                  VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q6581097"),
                  VALUE_FACTORY.createIRI(Vocabulary.REVISION_NAMESPACE, "42")
          ),
          VALUE_FACTORY.createStatement(
                  VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"),
                  VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"),
                  VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035"),
                  VALUE_FACTORY.createIRI(Vocabulary.REVISION_NAMESPACE, "42")
          ),
          VALUE_FACTORY.createStatement(
                  VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"),
                  VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"),
                  VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q19688263"),
                  VALUE_FACTORY.createIRI(Vocabulary.REVISION_NAMESPACE, "42")
          )
  );

  private final Path tempDir;

  public RocksTripleSourceTest() throws IOException {
    tempDir = Files.createTempDirectory(null);
    Files.deleteIfExists(tempDir);
  }

  @Before
  public void setUpBeforeClass() throws NotSupportedValueException {
    try (RocksStore store = new RocksStore(tempDir, false)) {
      NumericValueFactory factory = new NumericValueFactory(store.getReadWriteStringStore());
      for (Statement statement : STATEMENTS) {
        long s = factory.encodeValue(statement.getSubject());
        long p = factory.encodeValue(statement.getPredicate());
        long o = factory.encodeValue(statement.getObject());
        long revision = Long.valueOf(((IRI) statement.getContext()).getLocalName());
        long[] value = new long[]{revision, revision};
        store.spoStatementIndex().put(new long[]{s, p, o}, value);
        store.posStatementIndex().put(new long[]{p, o, s}, value);
        store.ospStatementIndex().put(new long[]{o, s, p}, value);
      }
    }
  }

  @Test
  public void testTriplePattern() {
    try (RocksTripleSource tripleSource = new RocksTripleSource(tempDir)) {
      assertLength(tripleSource.getStatements(null, null, null), 8);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), null, null), 8);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), null), 4);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035")), 2);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), null, VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035")), 2);
      assertLength(tripleSource.getStatements(null, VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), null), 4);
      assertLength(tripleSource.getStatements(null, VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035")), 2);
      assertLength(tripleSource.getStatements(null, null, VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035")), 2);

      Resource[] revision = new Resource[]{VALUE_FACTORY.createIRI(Vocabulary.REVISION_ADDITIONS_NAMESPACE, "42")};
      assertLength(tripleSource.getStatements(null, null, null, revision), 4);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), null, null, revision), 4);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), null, revision), 2);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035"), revision), 1);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), null, VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035"), revision), 1);
      assertLength(tripleSource.getStatements(null, VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), null, revision), 2);
      assertLength(tripleSource.getStatements(null, VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035"), revision), 1);
      assertLength(tripleSource.getStatements(null, null, VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035"), revision), 1);

    }
  }

  private static <X, E extends Exception> void assertLength(CloseableIteration<X, E> iteration, int length) throws E {
    try (CloseableIteration<X, E> iter = iteration) {
      int count = 0;
      while (iter.hasNext()) {
        count++;
        iter.next();
      }
      Assert.assertEquals(length, count);
    }
  }
}
