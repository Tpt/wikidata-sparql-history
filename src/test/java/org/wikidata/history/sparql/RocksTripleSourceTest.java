package org.wikidata.history.sparql;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

class RocksTripleSourceTest {

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

  RocksTripleSourceTest() throws IOException {
    tempDir = Files.createTempDirectory(null);
    Files.deleteIfExists(tempDir);
  }

  @BeforeEach
  void setUpBeforeClass() throws NotSupportedValueException {
    try (RocksStore store = new RocksStore(tempDir, false)) {
      NumericValueFactory factory = new NumericValueFactory(store.getReadWriteStringStore());
      for (Statement statement : STATEMENTS) {
        long s = factory.encodeValue(statement.getSubject());
        long p = factory.encodeValue(statement.getPredicate());
        long o = factory.encodeValue(statement.getObject());
        long revision = Long.parseLong(((IRI) statement.getContext()).getLocalName());
        long[] value = new long[]{revision, revision + 1};
        store.spoStatementIndex().put(new long[]{s, p, o}, value);
        store.posStatementIndex().put(new long[]{p, o, s}, value);
        store.ospStatementIndex().put(new long[]{o, s, p}, value);
        addToStatementListIndex(store.insertedStatementIndex(), revision, new long[]{s, p, o});
        addToStatementListIndex(store.deletedStatementIndex(), revision + 1, new long[]{s, p, o});
      }
    }
  }

  private static void addToStatementListIndex(RocksStore.Index<Long, long[]> index, long key, long[] triple) {
    long[] existingTriples = index.get(key);
    long[] newTriples = (existingTriples == null) ? triple : TripleArrayUtils.addToSortedArray(existingTriples, triple);
    if (newTriples != existingTriples) {
      index.put(key, newTriples);
    }
  }

  @Test
  void testTriplePattern() {
    try (RocksTripleSource tripleSource = new RocksTripleSource(tempDir)) {
      assertLength(tripleSource.getStatements(null, null, null), 8);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), null, null), 8);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), null), 4);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035")), 2);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), null, VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035")), 2);
      assertLength(tripleSource.getStatements(null, VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), null), 4);
      assertLength(tripleSource.getStatements(null, VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035")), 2);
      assertLength(tripleSource.getStatements(null, null, VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035")), 2);

      Resource[] insertionRevision = new Resource[]{VALUE_FACTORY.createIRI(Vocabulary.REVISION_ADDITIONS_NAMESPACE, "42")};
      assertLength(tripleSource.getStatements(null, null, null, insertionRevision), 4);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), null, null, insertionRevision), 4);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), null, insertionRevision), 2);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035"), insertionRevision), 1);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), null, VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035"), insertionRevision), 1);
      assertLength(tripleSource.getStatements(null, VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), null, insertionRevision), 2);
      assertLength(tripleSource.getStatements(null, VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035"), insertionRevision), 1);
      assertLength(tripleSource.getStatements(null, null, VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035"), insertionRevision), 1);

      Resource[] globalState1Revision = new Resource[]{VALUE_FACTORY.createIRI(Vocabulary.REVISION_GLOBAL_STATE_NAMESPACE, "42")};
      assertLength(tripleSource.getStatements(null, null, null, globalState1Revision), 4);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), null, null, globalState1Revision), 4);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), null, globalState1Revision), 2);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035"), globalState1Revision), 1);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), null, VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035"), globalState1Revision), 1);
      assertLength(tripleSource.getStatements(null, VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), null, globalState1Revision), 2);
      assertLength(tripleSource.getStatements(null, VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035"), globalState1Revision), 1);
      assertLength(tripleSource.getStatements(null, null, VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035"), globalState1Revision), 1);

      Resource[] globalState2Revision = new Resource[]{VALUE_FACTORY.createIRI(Vocabulary.REVISION_GLOBAL_STATE_NAMESPACE, "43")};
      assertLength(tripleSource.getStatements(null, null, null, globalState2Revision), 0);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), null, null, globalState2Revision), 0);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), null, globalState2Revision), 0);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035"), globalState2Revision), 0);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), null, VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035"), globalState2Revision), 0);
      assertLength(tripleSource.getStatements(null, VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), null, globalState2Revision), 0);
      assertLength(tripleSource.getStatements(null, VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035"), globalState2Revision), 0);
      assertLength(tripleSource.getStatements(null, null, VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035"), globalState2Revision), 0);

      Resource[] deletionRevision = new Resource[]{VALUE_FACTORY.createIRI(Vocabulary.REVISION_DELETIONS_NAMESPACE, "43")};
      assertLength(tripleSource.getStatements(null, null, null, deletionRevision), 4);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), null, null, deletionRevision), 4);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), null, deletionRevision), 2);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035"), deletionRevision), 1);
      assertLength(tripleSource.getStatements(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q42"), null, VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035"), deletionRevision), 1);
      assertLength(tripleSource.getStatements(null, VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), null, deletionRevision), 2);
      assertLength(tripleSource.getStatements(null, VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, "P735"), VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035"), deletionRevision), 1);
      assertLength(tripleSource.getStatements(null, null, VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, "Q463035"), deletionRevision), 1);
    }
  }

  private static <X, E extends Exception> void assertLength(CloseableIteration<X, E> iteration, int length) throws E {
    try (CloseableIteration<X, E> iter = iteration) {
      int count = 0;
      while (iter.hasNext()) {
        count++;
        iter.next();
      }
      Assertions.assertEquals(length, count);
    }
  }
}
