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

public final class RocksTripleSource implements TripleSource, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger("tripleSource");
  private static final CloseableIteration<Statement, QueryEvaluationException> EMPTY_ITERATION = new EmptyIteration<>();
  private static final long[] EMPTY_ARRAY = new long[]{};

  private final RocksStore store;
  private final RocksStore.Index<Long, Long> revisionDateIndex;
  private final RocksStore.Index<Long, long[]> dateRevisionsIndex;
  private final RocksStore.Index<Long, Long> parentRevisionIndex;
  private final RocksStore.Index<Long, Long> childRevisionIndex;
  private final RocksStore.Index<Long, Long> revisionTopicIndex;
  private final RocksStore.Index<Long, long[]> topicRevisionsIndex;
  private final RocksStore.Index<Long, String> revisionContributorIndex;
  private final RocksStore.Index<long[], long[]> spoStatementIndex;
  private final RocksStore.Index<long[], long[]> posStatementIndex;
  private final NumericValueFactory valueFactory;

  public RocksTripleSource(Path path) {
    store = new RocksStore(path, true);
    revisionDateIndex = store.revisionDateIndex();
    dateRevisionsIndex = store.dateRevisionsIndex();
    parentRevisionIndex = store.parentRevisionIndex();
    childRevisionIndex = store.childRevisionIndex();
    revisionTopicIndex = store.revisionTopicIndex();
    topicRevisionsIndex = store.topicRevisionIndex();
    revisionContributorIndex = store.revisionContributorIndex();
    spoStatementIndex = store.spoStatementIndex();
    posStatementIndex = store.posStatementIndex();
    valueFactory = new NumericValueFactory(store.getReadOnlyStringStore());
  }

  @Override
  public void close() {
    valueFactory.close();
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
        if (obj == null) {
          throw new QueryEvaluationException("not supported yet: ? " + pred + " ?"); //TODO
        } else {
          try {
            long[] revisions = dateRevisionsIndex.getOrDefault(Instant.parse(obj.stringValue()).getEpochSecond(), EMPTY_ARRAY);
            return new CloseableIteratorIteration<>(Arrays.stream(revisions)
                    .mapToObj(revisionId -> valueFactory.createStatement(valueFactory.createRevisionIRI(revisionId), Vocabulary.SCHEMA_ABOUT, obj))
                    .iterator()
            );
          } catch (DateTimeParseException e) {
            throw new QueryEvaluationException(pred + " is an invalid revision timestamp");
          }
        }
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
            long[] prefix = (obj == null)
                    ? new long[]{valueFactory.encodeValue(pred)}
                    : new long[]{valueFactory.encodeValue(pred), valueFactory.encodeValue(obj)};
            return new FlatMapClosableIteration<>(posStatementIndex.longPrefixIteration(
                    prefix,
                    (triple, revisions) -> revisionsInExpected(revisions, revisionIris).map(revisionIri -> formatPosTriple(triple, revisionIri)).iterator()));
          }
        } else {
          long[] prefix = (pred == null)
                  ? new long[]{valueFactory.encodeValue(subj)}
                  : (obj == null)
                  ? new long[]{valueFactory.encodeValue(subj), valueFactory.encodeValue(pred)}
                  : new long[]{valueFactory.encodeValue(subj), valueFactory.encodeValue(pred), valueFactory.encodeValue(obj)};
          CloseableIteration<Statement, QueryEvaluationException> result = new FlatMapClosableIteration<>(spoStatementIndex.longPrefixIteration(
                  prefix,
                  (triple, revisions) -> revisionsInExpected(revisions, revisionIris).map(revisionIri -> formatSpoTriple(triple, revisionIri)).iterator()));
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

  private Statement formatSpoTriple(long[] triple, Resource context) {
    try {
      return valueFactory.createStatement(
              (Resource) valueFactory.createValue(triple[0]),
              (IRI) valueFactory.createValue(triple[1]),
              valueFactory.createValue(triple[2]),
              context
      );
    } catch (NotSupportedValueException e) {
      throw new QueryEvaluationException(e);
    }
  }

  private Statement formatPosTriple(long[] triple, Resource context) {
    try {
      return valueFactory.createStatement(
              (Resource) valueFactory.createValue(triple[2]),
              (IRI) valueFactory.createValue(triple[0]),
              valueFactory.createValue(triple[1]),
              context
      );
    } catch (NotSupportedValueException e) {
      throw new QueryEvaluationException(e);
    }
  }

  private Stream<NumericValueFactory.RevisionIRI> revisionsInExpected(long[] actualRevisions, NumericValueFactory.RevisionIRI[] expectedRevisionRange) {
    return (expectedRevisionRange == null)
            ? boundRevisionOfRange(actualRevisions)
            : Arrays.stream(expectedRevisionRange).filter(revisionIri -> isInRanges(revisionIri, actualRevisions));
  }

  private Stream<NumericValueFactory.RevisionIRI> boundRevisionOfRange(long[] revisionRange) {
    List<NumericValueFactory.RevisionIRI> graphs = new ArrayList<>(revisionRange.length);
    for (int i = 0; i < revisionRange.length; i += 2) {
      graphs.add(valueFactory.createRevisionIRI(revisionRange[i], Vocabulary.SnapshotType.ADDITIONS));
      if (revisionRange[i + 1] != Long.MAX_VALUE) {
        graphs.add(valueFactory.createRevisionIRI(revisionRange[i + 1], Vocabulary.SnapshotType.DELETIONS));
      }
    }
    return graphs.stream();
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

  private static class FlatMapClosableIteration<E, X extends Exception> implements CloseableIteration<E, X> {
    private final CloseableIteration<Iterator<E>, X> iter;
    private Iterator<E> current;

    FlatMapClosableIteration(CloseableIteration<Iterator<E>, X> iter) {
      this.iter = iter;
      current = Collections.emptyIterator();
    }

    @Override
    public boolean hasNext() throws X {
      while (!current.hasNext() && iter.hasNext()) {
        current = iter.next();
      }
      return current.hasNext();
    }

    @Override
    public E next() throws X {
        while (!current.hasNext()) {
          current = iter.next();
        }
        return current.next();
      }

    @Override
    public void remove() {
    }

    @Override
    public void close() throws X {
      iter.close();
    }
  }
}
