package org.wikidata.history.sparql;


import org.eclipse.rdf4j.common.iteration.*;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Stream;

public final class MapDBTripleSource implements TripleSource, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger("tripleSource");
  private static final CloseableIteration<Statement, QueryEvaluationException> EMPTY_ITERATION = new EmptyIteration<>();
  private static final long[] EMPTY_ARRAY = new long[]{};

  private final MapDBStore store;
  private final MapDBStore.IndexReader<Long, Long> revisionDateIndex;
  private final MapDBStore.IndexReader<Long, Long> parentRevisionIndex;
  private final MapDBStore.IndexReader<Long, Long> childRevisionIndex;
  private final MapDBStore.IndexReader<Long, Long> revisionTopicIndex;
  private final MapDBStore.IndexReader<Long, long[]> topicRevisionsIndex;
  private final MapDBStore.IndexReader<Long, String> revisionContributorIndex;
  private final MapDBStore.IndexReader<NumericTriple, long[]> spoStatementIndex;
  private final MapDBStore.IndexReader<NumericTriple, long[]> posStatementIndex;
  private final NumericValueFactory valueFactory;

  public MapDBTripleSource(Path path) {
    store = new MapDBStore(path);
    revisionDateIndex = store.revisionDateIndex().newReader();
    parentRevisionIndex = store.parentRevisionIndex().newReader();
    childRevisionIndex = store.childRevisionIndex().newReader();
    revisionTopicIndex = store.revisionTopicIndex().newReader();
    topicRevisionsIndex = store.topicRevisionIndex().newReader();
    revisionContributorIndex = store.revisionContributorIndex().newReader();
    spoStatementIndex = store.spoStatementIndex().newReader();
    posStatementIndex = store.posStatementIndex().newReader();
    valueFactory = new NumericValueFactory(store.openStringStore());
  }

  @Override
  public void close() {
    valueFactory.close();
    revisionDateIndex.close();
    parentRevisionIndex.close();
    childRevisionIndex.close();
    revisionTopicIndex.close();
    topicRevisionsIndex.close();
    revisionContributorIndex.close();
    spoStatementIndex.close();
    posStatementIndex.close();
    store.close();
  }

  @Override
  public ValueFactory getValueFactory() {
    return valueFactory;
  }


  @Override
  public CloseableIteration<Statement, QueryEvaluationException> getStatements(
          Resource subj, IRI pred, Value obj, Resource... contexts
  ) throws QueryEvaluationException {
    if (Vocabulary.HISTORY_GLOBAL_STATE.equals(pred)) {
      return getStatementsForRevisionConversionRelation(subj, pred, obj, Vocabulary.SnapshotType.NONE, Vocabulary.SnapshotType.GLOBAL_STATE);
    } else if (Vocabulary.HISTORY_ADDITIONS.equals(pred)) {
      return getStatementsForRevisionConversionRelation(subj, pred, obj, Vocabulary.SnapshotType.NONE, Vocabulary.SnapshotType.ADDITIONS);
    } else if (Vocabulary.HISTORY_DELETIONS.equals(pred)) {
      return getStatementsForRevisionConversionRelation(subj, pred, obj, Vocabulary.SnapshotType.NONE, Vocabulary.SnapshotType.DELETIONS);
    } else if (Vocabulary.HISTORY_REVISION_ID.equals(pred)) {
      if (subj == null) {
        if (obj == null) {
          throw new QueryEvaluationException("not supported yet ? " + pred + " ?"); //TODO
        } else if (obj instanceof Literal) {
          return toIteration(valueFactory.createRevisionIRI(((Literal) obj).longValue()), pred, obj);
        } else {
          return EMPTY_ITERATION;
        }
      } else {
        NumericValueFactory.RevisionIRI subjRevision = convertRevisionIRI(subj);
        if (obj == null) {
          return toIteration(subj, pred, valueFactory.createLiteral(subjRevision.getRevisionId()));
        } else {
          return valueFactory.createLiteral(subjRevision.getRevisionId()).equals(obj)
                  ? toIteration(subj, pred, obj)
                  : EMPTY_ITERATION;
        }
      }
    } else if (Vocabulary.HISTORY_PREVIOUS_REVISION.equals(pred)) {
      if (subj == null) {
        if (obj == null) {
          throw new QueryEvaluationException("not supported yet ? " + pred + " ?"); //TODO
        } else {
          return toIteration(convertRevisionIRI(obj).previousRevision(), pred, obj);
        }
      } else {
        if (obj == null) {
          return toIteration(subj, pred, convertRevisionIRI(subj).previousRevision());
        } else {
          return convertRevisionIRI(subj).previousRevision().equals(obj)
                  ? toIteration(subj, pred, obj)
                  : EMPTY_ITERATION;
        }
      }
    } else if (Vocabulary.HISTORY_NEXT_REVISION.equals(pred)) {
      if (subj == null) {
        if (obj == null) {
          throw new QueryEvaluationException("not supported yet ? " + pred + " ?"); //TODO
        } else {
          return toIteration(convertRevisionIRI(obj).nextRevision(), pred, obj);
        }
      } else {
        if (obj == null) {
          return toIteration(subj, pred, convertRevisionIRI(subj).nextRevision());
        } else {
          return convertRevisionIRI(subj).nextRevision().equals(obj)
                  ? toIteration(subj, pred, obj)
                  : EMPTY_ITERATION;
        }
      }
    } else if (Vocabulary.SCHEMA_ABOUT.equals(pred)) {
      if (subj == null) {
        if (obj == null) {
          throw new QueryEvaluationException("not supported yet: ? " + pred + " ?"); //TODO
        } else {
          try {
            long[] revisions = topicRevisionsIndex.getOrDefault(valueFactory.encodeValue(obj), EMPTY_ARRAY);
            return new CloseableIteratorIteration<>(Arrays.stream(revisions)
                    .mapToObj(revisionId -> valueFactory.createStatement(valueFactory.createRevisionIRI(revisionId), Vocabulary.SCHEMA_ABOUT, obj))
                    .iterator()
            );
          } catch (NotSupportedValueException e) {
            LOGGER.error(e.getMessage(), e);
            return EMPTY_ITERATION;
          }
        }
      } else {
        if (obj == null) {
          return Optional.ofNullable(revisionTopicIndex.get(parseRevisionIRI(subj)))
                  .flatMap(value -> {
                    try {
                      return Optional.of(valueFactory.createValue(value));
                    } catch (NotSupportedValueException e) {
                      LOGGER.error(e.getMessage(), e);
                      return Optional.empty();
                    }
                  })
                  .map(newObj -> toIteration(subj, pred, newObj))
                  .orElse(EMPTY_ITERATION);
        } else {
          try {
            return revisionTopicIndex.getOrDefault(parseRevisionIRI(subj), 0L) == valueFactory.encodeValue(obj)
                    ? toIteration(subj, pred, obj)
                    : EMPTY_ITERATION;
          } catch (NotSupportedValueException e) {
            LOGGER.error(e.getMessage(), e);
            return EMPTY_ITERATION;
          }
        }
      }
    } else if (Vocabulary.SCHEMA_AUTHOR.equals(pred)) {
      if (subj == null) {
        throw new QueryEvaluationException("not supported yet: ? " + pred + " " + obj); //TODO
      } else {
        if (obj == null) {
          return Optional.ofNullable(revisionContributorIndex.get(parseRevisionIRI(subj)))
                  .map(valueFactory::createLiteral)
                  .map(newObj -> toIteration(subj, pred, newObj))
                  .orElse(EMPTY_ITERATION);
        } else {
          return obj.stringValue().equals(revisionContributorIndex.get(parseRevisionIRI(subj)))
                  ? toIteration(subj, pred, obj)
                  : EMPTY_ITERATION;
        }
      }
    } else if (Vocabulary.SCHEMA_DATE_CREATED.equals(pred)) {
      if (subj == null) {
        throw new QueryEvaluationException("not supported yet: ? " + pred + " " + obj); //TODO
      } else {
        if (obj == null) {
          return Optional.ofNullable(revisionDateIndex.get(parseRevisionIRI(subj)))
                  .map(timestamp -> valueFactory.createLiteral(
                          Instant.ofEpochSecond(timestamp).toString(), XMLSchema.DATETIME
                  ))
                  .map(newObj -> toIteration(subj, pred, newObj))
                  .orElse(EMPTY_ITERATION);
        } else {
          try {
            return revisionDateIndex.getOrDefault(parseRevisionIRI(subj), 0L) == Instant.parse(obj.stringValue()).getEpochSecond()
                    ? toIteration(subj, pred, obj)
                    : EMPTY_ITERATION;
          } catch (DateTimeParseException e) {
            throw new QueryEvaluationException(pred + " is an invalid revision timestamp");
          }
        }
      }
    } else if (Vocabulary.SCHEMA_IS_BASED_ON.equals(pred)) {
      if (subj == null) {
        if (obj == null) {
          throw new QueryEvaluationException("not supported yet: ? " + pred + " ?"); //TODO
        } else {
          return Optional.ofNullable(childRevisionIndex.get(parseRevisionIRI(obj)))
                  .map(valueFactory::createRevisionIRI)
                  .map(newSubj -> toIteration(newSubj, pred, obj))
                  .orElse(EMPTY_ITERATION);
        }
      } else {
        if (obj == null) {
          return Optional.ofNullable(parentRevisionIndex.get(parseRevisionIRI(subj)))
                  .map(valueFactory::createRevisionIRI)
                  .map(newObj -> toIteration(subj, pred, newObj))
                  .orElse(EMPTY_ITERATION);
        } else {
          return parentRevisionIndex.getOrDefault(parseRevisionIRI(subj), 0L) == parseRevisionIRI(obj)
                  ? toIteration(subj, pred, obj)
                  : EMPTY_ITERATION;
        }
      }
    } else {
      if (pred != null && !pred.equals(OWL.SAMEAS) && !pred.equals(Vocabulary.P279_CLOSURE) && !pred.getNamespace().equals(Vocabulary.WDT_NAMESPACE)) {
        return new EmptyIteration<>(); //TODO: better filter
      }
      try {
        NumericValueFactory.RevisionIRI[] revisionIris = getRevisionIris(contexts);
        if (subj == null) {
          if (pred == null) {
            throw new QueryEvaluationException("NumericTriple patterns with not subject and predicate are not supported");
          } else {
            return new DirectStatementIteration(posStatementIndex.entryIterator(new NumericTriple(
                    0,
                    valueFactory.encodeValue(pred),
                    (obj == null) ? 0 : valueFactory.encodeValue(obj)
            )), revisionIris);
          }
        } else {
          CloseableIteration<Statement, QueryEvaluationException> result = new DirectStatementIteration(spoStatementIndex.entryIterator(new NumericTriple(
                  valueFactory.encodeValue(subj),
                  (pred == null) ? 0 : valueFactory.encodeValue(pred),
                  (pred == null || obj == null) ? 0 : valueFactory.encodeValue(obj)
          )), revisionIris);
          if (pred == null && obj != null) {
            return new FilterIteration<Statement, QueryEvaluationException>(result) {
              @Override
              protected boolean accept(Statement statement) throws QueryEvaluationException {
                return statement.getObject().equals(obj);
              }
            };
          } else {
            return result;
          }
        }
      } catch (NotSupportedValueException e) {
        throw new QueryEvaluationException(e);
      }
    }
  }

  private CloseableIteration<Statement, QueryEvaluationException> getStatementsForRevisionConversionRelation(
          Resource subj, IRI pred, Value obj,
          Vocabulary.SnapshotType subjType, Vocabulary.SnapshotType objType
  ) {
    if (subj == null) {
      if (obj == null) {
        throw new QueryEvaluationException("not supported yet ? " + pred + " ?"); //TODO
      } else {
        NumericValueFactory.RevisionIRI objRevision = convertRevisionIRI(obj);
        return objRevision.getSnapshotType() == objType
                ? toIteration(objRevision.withSnapshotType(subjType), pred, obj)
                : EMPTY_ITERATION;
      }
    } else {
      NumericValueFactory.RevisionIRI subjRevision = convertRevisionIRI(subj);
      if (subjRevision.getSnapshotType() != subjType) {
        return EMPTY_ITERATION;
      } else if (obj == null) {
        return toIteration(subj, pred, subjRevision.withSnapshotType(objType));
      } else {
        return subjRevision.withSnapshotType(objType).equals(obj)
                ? toIteration(subj, pred, obj)
                : EMPTY_ITERATION;
      }
    }
  }

  private NumericValueFactory.RevisionIRI[] getRevisionIris(Resource... contexts) {
    if (contexts == null || contexts.length == 0) {
      //LOGGER.info("No revision context given");
      return null;
    }
    return Arrays.stream(contexts).map(this::convertRevisionIRI).toArray(NumericValueFactory.RevisionIRI[]::new);
  }

  private Statement formatTriple(NumericTriple triple, Resource context) {
    try {
      return valueFactory.createStatement(
              (Resource) valueFactory.createValue(triple.getSubject()),
              (IRI) valueFactory.createValue(triple.getPredicate()),
              valueFactory.createValue(triple.getObject()),
              context
      );
    } catch (NotSupportedValueException e) {
      throw new QueryEvaluationException(e);
    }
  }

  private NumericValueFactory.RevisionIRI convertRevisionIRI(Value revisionIRI) {
    if (!(revisionIRI instanceof NumericValueFactory.RevisionIRI) && revisionIRI instanceof IRI) {
      revisionIRI = valueFactory.createIRI((IRI) revisionIRI);
    }
    if (!(revisionIRI instanceof NumericValueFactory.RevisionIRI)) {
      throw new QueryEvaluationException("Not supported revision IRI: " + revisionIRI);
    }
    return (NumericValueFactory.RevisionIRI) revisionIRI;
  }

  private long parseRevisionIRI(Value revisionIRI) {
    return convertRevisionIRI(revisionIRI).getRevisionId();
  }

  private boolean isInRanges(NumericValueFactory.RevisionIRI revisionIri, long[] revisionIdRanges) {
    switch (revisionIri.getSnapshotType()) {
      case GLOBAL_STATE:
        return LongRangeUtils.isInRange(revisionIri.getRevisionId(), revisionIdRanges);
      case ADDITIONS:
        return LongRangeUtils.isRangeStart(revisionIri.getRevisionId(), revisionIdRanges);
      case DELETIONS:
        return LongRangeUtils.isRangeEnd(revisionIri.getRevisionId(), revisionIdRanges);
      default:
        LOGGER.warn("Not supported snapshot type: " + revisionIri.getSnapshotType());
        return false;
    }
  }

  private CloseableIteration<Statement, QueryEvaluationException> toIteration(Resource subj, IRI pred, Value obj) {
    if (subj == null || pred == null || obj == null) {
      return EMPTY_ITERATION;
    } else {
      return new SingletonIteration<>(valueFactory.createStatement(subj, pred, obj));
    }
  }

  private abstract static class MultipleConvertingIteration<S, T, X extends Exception> extends AbstractCloseableIteration<T, X> {
    private final Iteration<? extends S, ? extends X> iter;
    private Iterator<? extends T> current;

    MultipleConvertingIteration(Iteration<? extends S, ? extends X> iter) {
      this.iter = iter;
      current = Collections.emptyIterator();
    }

    protected abstract Iterator<? extends T> convert(S var1) throws X;

    public boolean hasNext() throws X {
      if (isClosed()) {
        return false;
      } else {
        while (!current.hasNext() && iter.hasNext()) {
          current = convert(iter.next());
        }
        boolean result = current.hasNext();
        if (!result) {
          close();
        }
        return result;
      }
    }

    public T next() throws X {
      if (isClosed()) {
        throw new NoSuchElementException("The iteration has been closed.");
      } else {
        while (!current.hasNext()) {
          current = convert(iter.next());
        }
        return current.next();
      }
    }

    public void remove() throws X {
      if (isClosed()) {
        throw new IllegalStateException("The iteration has been closed.");
      } else {
        iter.remove();
      }
    }

    protected void handleClose() throws X {
      try {
        super.handleClose();
      } finally {
        Iterations.closeCloseable(iter);
      }
    }
  }

  private final class DirectStatementIteration extends MultipleConvertingIteration<Map.Entry<NumericTriple, long[]>, Statement, QueryEvaluationException> {
    private NumericValueFactory.RevisionIRI[] revisionIris;

    DirectStatementIteration(Iterator<Map.Entry<NumericTriple, long[]>> iter, NumericValueFactory.RevisionIRI[] revisionIris) {
      super(new CloseableIteratorIteration<>(iter));
      this.revisionIris = revisionIris;
    }

    @Override
    protected Iterator<Statement> convert(Map.Entry<NumericTriple, long[]> entry) throws QueryEvaluationException {
      return findRevisions(entry.getValue()).map(revisionIri -> formatTriple(entry.getKey(), revisionIri)).iterator();
    }

    private Stream<NumericValueFactory.RevisionIRI> findRevisions(long[] statementRevisions) {
      return (revisionIris == null)
              ? defaultGraphs(statementRevisions)
              : Arrays.stream(revisionIris).filter(revisionIri -> isInRanges(revisionIri, statementRevisions));
    }

    private Stream<NumericValueFactory.RevisionIRI> defaultGraphs(long[] statementRevisions) {
      List<NumericValueFactory.RevisionIRI> graphs = new ArrayList<>(statementRevisions.length);
      for (int i = 0; i < statementRevisions.length; i += 2) {
        graphs.add(valueFactory.createRevisionIRI(statementRevisions[i], Vocabulary.SnapshotType.ADDITIONS));
        if (statementRevisions[i + 1] != Long.MAX_VALUE) {
          graphs.add(valueFactory.createRevisionIRI(statementRevisions[i + 1], Vocabulary.SnapshotType.DELETIONS));
        }
      }
      return graphs.stream();
    }
  }
}
