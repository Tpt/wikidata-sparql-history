package org.wikidata.history.sparql;


import org.eclipse.rdf4j.common.iteration.*;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;
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
  private final RocksStore.Index<Map.Entry<String, Long>, Object> contributorRevisionsIndex;
  private final RocksStore.Index<long[], long[]> spoStatementIndex;
  private final RocksStore.Index<long[], long[]> posStatementIndex;
  private final RocksStore.Index<long[], long[]> ospStatementIndex;
  private final RocksStore.Index<Long, long[]> insertedStatementIndex;
  private final RocksStore.Index<Long, long[]> deletedStatementIndex;
  private final NumericValueFactory valueFactory;
  private final Map<IRI, MagicPredicate> magicPredicates = new HashMap<>();

  public RocksTripleSource(Path path) {
    store = new RocksStore(path, true);
    revisionDateIndex = store.revisionDateIndex();
    dateRevisionsIndex = store.dateRevisionsIndex();
    parentRevisionIndex = store.parentRevisionIndex();
    childRevisionIndex = store.childRevisionIndex();
    revisionTopicIndex = store.revisionTopicIndex();
    topicRevisionsIndex = store.topicRevisionIndex();
    revisionContributorIndex = store.revisionContributorIndex();
    contributorRevisionsIndex = store.contributorRevisionsIndex();
    spoStatementIndex = store.spoStatementIndex();
    posStatementIndex = store.posStatementIndex();
    ospStatementIndex = store.ospStatementIndex();
    insertedStatementIndex = store.insertedStatementIndex();
    deletedStatementIndex = store.deletedStatementIndex();
    valueFactory = new NumericValueFactory(store.getReadOnlyStringStore());
    registerMagicPredicates();
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

  private void registerMagicPredicates() {
    MagicPredicate[] predicates = new MagicPredicate[]{
            new RevisionsStatesConverter(Vocabulary.HISTORY_GLOBAL_STATE, Vocabulary.SnapshotType.NONE, Vocabulary.SnapshotType.GLOBAL_STATE),
            new RevisionsStatesConverter(Vocabulary.HISTORY_ADDITIONS, Vocabulary.SnapshotType.NONE, Vocabulary.SnapshotType.ADDITIONS),
            new RevisionsStatesConverter(Vocabulary.HISTORY_DELETIONS, Vocabulary.SnapshotType.NONE, Vocabulary.SnapshotType.DELETIONS),
            new RevisionIdMagicPredicate(),
            new PreviousRevisionMagicPredicate(),
            new NextRevisionMagicPredicate(),
            new RevisionTopicMagicPredicate(),
            new RevisionDateMagicPredicate(),
            new ParentRevisionMagicPredicate(),
            new RevisionAuthorMagicPredicate(),
            new RevisionGlobalStateAtMagicPredicate()
    };
    for (MagicPredicate predicate : predicates) {
      magicPredicates.put(predicate.getPredicate(), predicate);
    }
  }

  @Override
  public CloseableIteration<Statement, QueryEvaluationException> getStatements(
          Resource subj, IRI pred, Value obj, Resource... contexts
  ) throws QueryEvaluationException {
    if (contexts == null || contexts.length == 0) {
      return getStatements(subj, pred, obj, (Resource) null);
    } else if (contexts.length == 1) {
      return getStatements(subj, pred, obj, contexts[0]);
    } else {
      List<CloseableIteration<Statement, QueryEvaluationException>> results = new ArrayList<>(contexts.length);
      for (Resource context : contexts) {
        results.add(getStatements(subj, pred, obj, context));
      }
      return new UnionIteration<>(results);
    }
  }

  private CloseableIteration<Statement, QueryEvaluationException> getStatements(Resource subj, IRI pred, Value obj, Resource context) throws QueryEvaluationException {
    if (context != null) {
      NumericValueFactory.RevisionIRI revisionIri = convertRevisionIRI(context);
      if (revisionIri == null) {
        return new EmptyIteration<>(); //Invalid revision IRI
      } else {
        return getStatementsForBasicRelationWithGuess(subj, pred, obj, revisionIri);
      }
    } else if (pred == null) {
      return new UnionIteration<>(Stream.concat(
              magicPredicates.values().stream().map(predicate -> predicate.getStatements(subj, obj)),
              Stream.of(getStatementsForBasicRelationWithGuess(subj, null, obj, null))
      ).collect(Collectors.toList()));
    } else if (magicPredicates.containsKey(pred)) {
      return magicPredicates.get(pred).getStatements(subj, obj);
    } else {
      return getStatementsForBasicRelationWithGuess(subj, pred, obj, null);
    }
  }

  private CloseableIteration<Statement, QueryEvaluationException> getStatementsForBasicRelationWithGuess(Resource subj, IRI pred, Value obj, NumericValueFactory.RevisionIRI revisionIri) {
    // If we have a context we could restrict our triples patterns to triples in this context
    if (revisionIri != null && revisionIri.getSnapshotType() == Vocabulary.SnapshotType.ADDITIONS) {
      return getStatementsInTripleListIndex(subj, pred, obj, revisionIri, insertedStatementIndex);
    }
    if (revisionIri != null && revisionIri.getSnapshotType() == Vocabulary.SnapshotType.DELETIONS) {
      return getStatementsInTripleListIndex(subj, pred, obj, revisionIri, deletedStatementIndex);
    } else {
      return getStatementsForBasicRelation(subj, pred, obj, revisionIri);
    }
  }

  private CloseableIteration<Statement, QueryEvaluationException> getStatementsForBasicRelation(Resource subj, IRI pred, Value obj, NumericValueFactory.RevisionIRI revisionIri) {
    try {
      if (subj == null) {
        if (pred == null) {
          long[] prefix = (obj == null) ? EMPTY_ARRAY : new long[]{valueFactory.encodeValue(obj)};
          return new FlatMapClosableIteration<>(ospStatementIndex.longPrefixIteration(
                  prefix,
                  (triple, revisions) -> revisionsInExpected(revisions, revisionIri).map(rev -> formatOspTriple(triple, rev)).iterator()));
        } else {
          long[] prefix = (obj == null)
                  ? new long[]{valueFactory.encodeValue(pred)}
                  : new long[]{valueFactory.encodeValue(pred), valueFactory.encodeValue(obj)};
          return new FlatMapClosableIteration<>(posStatementIndex.longPrefixIteration(
                  prefix,
                  (triple, revisions) -> revisionsInExpected(revisions, revisionIri).map(rev -> formatPosTriple(triple, rev)).iterator()));
        }
      } else {
        if (obj == null) {
          long[] prefix = pred == null
                  ? new long[]{valueFactory.encodeValue(subj)}
                  : new long[]{valueFactory.encodeValue(subj), valueFactory.encodeValue(pred)};
          return new FlatMapClosableIteration<>(spoStatementIndex.longPrefixIteration(
                  prefix,
                  (triple, revisions) -> revisionsInExpected(revisions, revisionIri).map(rev -> formatSpoTriple(triple, rev)).iterator()));
        } else if (pred == null) {
          long[] prefix = new long[]{valueFactory.encodeValue(obj), valueFactory.encodeValue(subj)};
          return new FlatMapClosableIteration<>(ospStatementIndex.longPrefixIteration(
                  prefix,
                  (triple, revisions) -> revisionsInExpected(revisions, revisionIri).map(rev -> formatOspTriple(triple, rev)).iterator()));
        } else {
          long[] triple = new long[]{valueFactory.encodeValue(subj), valueFactory.encodeValue(pred), valueFactory.encodeValue(obj)};
          long[] revisions = spoStatementIndex.get(triple);
          return (revisions == null)
                  ? EMPTY_ITERATION
                  : new CloseableIteratorIteration<>(revisionsInExpected(revisions, revisionIri).map(rev -> formatSpoTriple(triple, rev)).iterator());
        }
      }
    } catch (NotSupportedValueException e) {
      throw new QueryEvaluationException(e);
    }
  }

  private CloseableIteration<Statement, QueryEvaluationException> getStatementsInTripleListIndex(Resource subj, IRI pred, Value obj, NumericValueFactory.RevisionIRI revisionIri, RocksStore.Index<Long, long[]> index) {
    long[] triples = index.get(revisionIri.getRevisionId());
    if (triples == null) {
      return EMPTY_ITERATION;
    }
    try {
      Long s = (subj == null) ? null : valueFactory.encodeValue(subj);
      Long p = (pred == null) ? null : valueFactory.encodeValue(pred);
      Long o = (obj == null) ? null : valueFactory.encodeValue(obj);
      return new TripleListIteration(triples, valueFactory, s, p, o, revisionIri);
    } catch (NotSupportedValueException e) {
      throw new QueryEvaluationException(e);
    }
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

  private Statement formatOspTriple(long[] triple, Resource context) {
    try {
      return valueFactory.createStatement(
              (Resource) valueFactory.createValue(triple[1]),
              (IRI) valueFactory.createValue(triple[2]),
              valueFactory.createValue(triple[0]),
              context
      );
    } catch (NotSupportedValueException e) {
      throw new QueryEvaluationException(e);
    }
  }

  private Stream<NumericValueFactory.RevisionIRI> revisionsInExpected(long[] actualRevisions, NumericValueFactory.RevisionIRI expectedRevision) {
    return (expectedRevision == null)
            ? boundRevisionOfRange(actualRevisions)
            : isInRanges(expectedRevision, actualRevisions) ? Stream.of(expectedRevision) : Stream.empty();
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
      return null;
    }
    return (NumericValueFactory.RevisionIRI) revisionIRI;
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

  private static class TripleListIteration implements CloseableIteration<Statement, QueryEvaluationException> {
    private final long[] triples;
    private final NumericValueFactory valueFactory;
    private final Long subject;
    private final Long predicate;
    private final Long object;
    private final Resource context;
    private int position = 0;

    TripleListIteration(long[] triples, NumericValueFactory valueFactory, Long subject, Long predicate, Long object, Resource context) {
      this.triples = triples;
      this.valueFactory = valueFactory;
      this.subject = subject;
      this.predicate = predicate;
      this.object = object;
      this.context = context;
    }

    @Override
    public boolean hasNext() {
      while (
              position < triples.length && (
                      (subject != null && triples[position] != subject) ||
                              (predicate != null && triples[position + 1] != predicate) ||
                              (object != null && triples[position + 2] != object)
              )) {
        position += 3;
      }
      return position < triples.length;
    }

    @Override
    public Statement next() throws QueryEvaluationException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      try {
        return valueFactory.createStatement(
                (Resource) valueFactory.createValue(triples[position]),
                (IRI) valueFactory.createValue(triples[position + 1]),
                valueFactory.createValue(triples[position + 2]),
                context
        );
      } catch (NotSupportedValueException e) {
        throw new QueryEvaluationException(e);
      } finally {
        position += 3;
      }
    }

    @Override
    public void remove() {
    }

    @Override
    public void close() {
    }
  }

  private abstract class MagicPredicate {
    private final IRI predicate;

    MagicPredicate(IRI predicate) {
      this.predicate = valueFactory.createIRI(predicate);
    }

    IRI getPredicate() {
      return predicate;
    }

    abstract CloseableIteration<Statement, QueryEvaluationException> getStatements(Resource subj, Value obj);
  }

  private abstract class RevisionsSubjectMagicPredicate extends MagicPredicate {
    RevisionsSubjectMagicPredicate(IRI predicate) {
      super(predicate);
    }

    @Override
    CloseableIteration<Statement, QueryEvaluationException> getStatements(Resource subj, Value obj) {
      if (subj == null) {
        return getStatements(null, obj);
      } else {
        NumericValueFactory.RevisionIRI revision = convertRevisionIRI(subj);
        if (revision == null) {
          return EMPTY_ITERATION;
        }
        return getStatements(revision, obj);
      }
    }

    abstract CloseableIteration<Statement, QueryEvaluationException> getStatements(NumericValueFactory.RevisionIRI subj, Value obj);
  }

  private abstract class BetweenRevisionsMagicPredicate extends MagicPredicate {
    BetweenRevisionsMagicPredicate(IRI predicate) {
      super(predicate);
    }

    @Override
    CloseableIteration<Statement, QueryEvaluationException> getStatements(Resource subj, Value obj) {
      if (subj == null) {
        if (obj == null) {
          return getStatements();
        }
        NumericValueFactory.RevisionIRI objRevision = convertRevisionIRI(obj);
        if (objRevision == null) {
          return EMPTY_ITERATION;
        }
        return getStatementsForObject(objRevision);
      } else {
        NumericValueFactory.RevisionIRI subjRevision = convertRevisionIRI(subj);
        if (subjRevision == null) {
          return EMPTY_ITERATION;
        }
        if (obj == null) {
          return getStatementsForSubject(subjRevision);
        }

        NumericValueFactory.RevisionIRI objRevision = convertRevisionIRI(obj);
        if (objRevision == null) {
          return EMPTY_ITERATION;
        }
        return getStatementsForSubjectObject(subjRevision, objRevision);
      }
    }

    abstract CloseableIteration<Statement, QueryEvaluationException> getStatements();

    abstract CloseableIteration<Statement, QueryEvaluationException> getStatementsForSubject(NumericValueFactory.RevisionIRI subj);

    abstract CloseableIteration<Statement, QueryEvaluationException> getStatementsForSubjectObject(NumericValueFactory.RevisionIRI subj, NumericValueFactory.RevisionIRI obj);

    abstract CloseableIteration<Statement, QueryEvaluationException> getStatementsForObject(NumericValueFactory.RevisionIRI obj);
  }

  private final class RevisionsStatesConverter extends BetweenRevisionsMagicPredicate {
    private final Vocabulary.SnapshotType subjType;
    private final Vocabulary.SnapshotType objType;

    RevisionsStatesConverter(IRI predicate, Vocabulary.SnapshotType subjType, Vocabulary.SnapshotType objType) {
      super(predicate);
      this.subjType = subjType;
      this.objType = objType;
    }

    @Override
    CloseableIteration<Statement, QueryEvaluationException> getStatements() {
      return revisionTopicIndex.longPrefixIteration(new long[]{}, (key, value) -> {
        NumericValueFactory.RevisionIRI rev = valueFactory.createRevisionIRI(key);
        return valueFactory.createStatement(rev, getPredicate(), rev.withSnapshotType(objType));
      });
    }

    @Override
    CloseableIteration<Statement, QueryEvaluationException> getStatementsForSubject(NumericValueFactory.RevisionIRI subj) {
      return toIteration(subj, getPredicate(), subj.withSnapshotType(objType));
    }

    @Override
    CloseableIteration<Statement, QueryEvaluationException> getStatementsForSubjectObject(NumericValueFactory.RevisionIRI subj, NumericValueFactory.RevisionIRI obj) {
      return subj.withSnapshotType(objType).equals(obj)
              ? toIteration(subj, getPredicate(), obj)
              : EMPTY_ITERATION;
    }

    @Override
    CloseableIteration<Statement, QueryEvaluationException> getStatementsForObject(NumericValueFactory.RevisionIRI obj) {
      return obj.getSnapshotType() == objType
              ? toIteration(obj.withSnapshotType(subjType), getPredicate(), obj)
              : EMPTY_ITERATION;
    }
  }

  private final class RevisionIdMagicPredicate extends RevisionsSubjectMagicPredicate {
    RevisionIdMagicPredicate() {
      super(Vocabulary.HISTORY_REVISION_ID);
    }

    @Override
    CloseableIteration<Statement, QueryEvaluationException> getStatements(NumericValueFactory.RevisionIRI subjRevision, Value obj) {
      if (subjRevision == null) {
        if (obj == null) {
          return revisionTopicIndex.longPrefixIteration(new long[]{}, (key, value) -> {
            NumericValueFactory.RevisionIRI rev = valueFactory.createRevisionIRI(key);
            return valueFactory.createStatement(rev, getPredicate(), valueFactory.createLiteral(rev.getRevisionId()));
          });
        } else if (obj instanceof Literal) {
          return toIteration(valueFactory.createRevisionIRI(((Literal) obj).longValue()), getPredicate(), obj);
        } else {
          return EMPTY_ITERATION;
        }
      } else {
        if (obj == null) {
          return toIteration(subjRevision, getPredicate(), valueFactory.createLiteral(subjRevision.getRevisionId()));
        } else {
          return valueFactory.createLiteral(subjRevision.getRevisionId()).equals(obj)
                  ? toIteration(subjRevision, getPredicate(), obj)
                  : EMPTY_ITERATION;
        }
      }
    }
  }

  private final class PreviousRevisionMagicPredicate extends BetweenRevisionsMagicPredicate {
    PreviousRevisionMagicPredicate() {
      super(Vocabulary.HISTORY_PREVIOUS_REVISION);
    }

    @Override
    CloseableIteration<Statement, QueryEvaluationException> getStatements() {
      return revisionTopicIndex.longPrefixIteration(new long[]{}, (key, value) -> {
        NumericValueFactory.RevisionIRI rev = valueFactory.createRevisionIRI(key);
        return valueFactory.createStatement(rev, getPredicate(), rev.previousRevision());
      });
    }

    @Override
    CloseableIteration<Statement, QueryEvaluationException> getStatementsForSubject(NumericValueFactory.RevisionIRI subj) {
      return toIteration(subj, getPredicate(), subj.previousRevision());
    }

    @Override
    CloseableIteration<Statement, QueryEvaluationException> getStatementsForSubjectObject(NumericValueFactory.RevisionIRI subj, NumericValueFactory.RevisionIRI obj) {
      return subj.nextRevision().equals(obj) ? toIteration(subj, getPredicate(), obj) : EMPTY_ITERATION;
    }

    @Override
    CloseableIteration<Statement, QueryEvaluationException> getStatementsForObject(NumericValueFactory.RevisionIRI obj) {
      return toIteration(obj.nextRevision(), getPredicate(), obj);
    }
  }

  private final class NextRevisionMagicPredicate extends BetweenRevisionsMagicPredicate {
    NextRevisionMagicPredicate() {
      super(Vocabulary.HISTORY_NEXT_REVISION);
    }

    @Override
    CloseableIteration<Statement, QueryEvaluationException> getStatements() {
      return revisionTopicIndex.longPrefixIteration(new long[]{}, (key, value) -> {
        NumericValueFactory.RevisionIRI rev = valueFactory.createRevisionIRI(key);
        return valueFactory.createStatement(rev, getPredicate(), rev.nextRevision());
      });
    }

    @Override
    CloseableIteration<Statement, QueryEvaluationException> getStatementsForSubject(NumericValueFactory.RevisionIRI subj) {
      return toIteration(subj, getPredicate(), subj.nextRevision());
    }

    @Override
    CloseableIteration<Statement, QueryEvaluationException> getStatementsForSubjectObject(NumericValueFactory.RevisionIRI subj, NumericValueFactory.RevisionIRI obj) {
      return subj.previousRevision().equals(obj) ? toIteration(subj, getPredicate(), obj) : EMPTY_ITERATION;
    }

    @Override
    CloseableIteration<Statement, QueryEvaluationException> getStatementsForObject(NumericValueFactory.RevisionIRI obj) {
      return toIteration(obj.previousRevision(), getPredicate(), obj);
    }
  }

  private abstract class RevisionPropertyMultipleAncestorsMagicPredicate extends RevisionsSubjectMagicPredicate {
    private final RocksStore.Index<Long, Long> soIndex;
    private final RocksStore.Index<Long, long[]> osIndex;

    RevisionPropertyMultipleAncestorsMagicPredicate(IRI predicate, RocksStore.Index<Long, Long> soIndex, RocksStore.Index<Long, long[]> osIndex) {
      super(predicate);
      this.soIndex = soIndex;
      this.osIndex = osIndex;
    }

    @Override
    CloseableIteration<Statement, QueryEvaluationException> getStatements(NumericValueFactory.RevisionIRI subjRevision, Value obj) {
      if (subjRevision == null) {
        if (obj == null) {
          return soIndex.longPrefixIteration(new long[]{}, (key, value) -> valueFactory.createStatement(
                  valueFactory.createRevisionIRI(key),
                  getPredicate(),
                  decodeValue(value)
          ));
        } else {
          OptionalLong value = encodeValue(obj);
          return value.isPresent() ? new CloseableIteratorIteration<>(Arrays.stream(osIndex.getOrDefault(value.getAsLong(), EMPTY_ARRAY))
                  .mapToObj(revisionId -> valueFactory.createStatement(valueFactory.createRevisionIRI(revisionId), getPredicate(), obj))
                  .iterator()
          ) : EMPTY_ITERATION;
        }
      } else {
        if (obj == null) {
          Long value = soIndex.get(subjRevision.getRevisionId());
          return value == null ? EMPTY_ITERATION : toIteration(subjRevision, getPredicate(), decodeValue(value));
        } else {
          OptionalLong value = encodeValue(obj);
          return value.isPresent() && revisionTopicIndex.getOrDefault(subjRevision.getRevisionId(), 0L) == value.getAsLong()
                  ? toIteration(subjRevision, getPredicate(), obj)
                  : EMPTY_ITERATION;
        }
      }
    }

    abstract OptionalLong encodeValue(Value value) throws QueryEvaluationException;

    abstract Value decodeValue(long value) throws QueryEvaluationException;
  }

  private final class RevisionTopicMagicPredicate extends RevisionPropertyMultipleAncestorsMagicPredicate {
    RevisionTopicMagicPredicate() {
      super(Vocabulary.SCHEMA_ABOUT, revisionTopicIndex, topicRevisionsIndex);
    }

    @Override
    OptionalLong encodeValue(Value value) throws QueryEvaluationException {
      try {
        return OptionalLong.of(valueFactory.encodeValue(value));
      } catch (NotSupportedValueException e) {
        throw new QueryEvaluationException(e);
      }
    }

    @Override
    Value decodeValue(long value) throws QueryEvaluationException {
      try {
        return valueFactory.createValue(value);
      } catch (NotSupportedValueException e) {
        throw new QueryEvaluationException(e);
      }
    }
  }

  private final class RevisionDateMagicPredicate extends RevisionPropertyMultipleAncestorsMagicPredicate {
    RevisionDateMagicPredicate() {
      super(Vocabulary.SCHEMA_DATE_CREATED, revisionDateIndex, dateRevisionsIndex);
    }

    @Override
    OptionalLong encodeValue(Value value) throws QueryEvaluationException {
      if (value instanceof Literal && ((Literal) value).getDatatype().equals(XMLSchema.DATETIME)) {
        try {
          return OptionalLong.of(Instant.parse(value.stringValue()).getEpochSecond());
        } catch (DateTimeParseException e) {
          throw new QueryEvaluationException(value + " is an invalid revision timestamp");
        }
      }
      return OptionalLong.empty();
    }

    @Override
    Value decodeValue(long value) throws QueryEvaluationException {
      return valueFactory.createLiteral(Instant.ofEpochSecond(value).toString(), XMLSchema.DATETIME);
    }
  }

  private final class ParentRevisionMagicPredicate extends BetweenRevisionsMagicPredicate {
    ParentRevisionMagicPredicate() {
      super(Vocabulary.SCHEMA_IS_BASED_ON);
    }

    @Override
    CloseableIteration<Statement, QueryEvaluationException> getStatements() {
      return parentRevisionIndex.longPrefixIteration(new long[]{}, (key, value) ->
              valueFactory.createStatement(valueFactory.createRevisionIRI(key), getPredicate(), valueFactory.createRevisionIRI(value))
      );
    }

    @Override
    CloseableIteration<Statement, QueryEvaluationException> getStatementsForSubject(NumericValueFactory.RevisionIRI subj) {
      Long obj = parentRevisionIndex.get(subj.getRevisionId());
      return obj == null ? EMPTY_ITERATION : toIteration(subj, getPredicate(), valueFactory.createRevisionIRI(obj));
    }

    @Override
    CloseableIteration<Statement, QueryEvaluationException> getStatementsForSubjectObject(NumericValueFactory.RevisionIRI subj, NumericValueFactory.RevisionIRI obj) {
      return parentRevisionIndex.getOrDefault(subj.getRevisionId(), 0L) == obj.getRevisionId()
              ? toIteration(subj, getPredicate(), obj)
              : EMPTY_ITERATION;
    }

    @Override
    CloseableIteration<Statement, QueryEvaluationException> getStatementsForObject(NumericValueFactory.RevisionIRI obj) {
      Long subj = childRevisionIndex.get(obj.getRevisionId());
      return subj == null ? EMPTY_ITERATION : toIteration(valueFactory.createRevisionIRI(subj), getPredicate(), obj);
    }
  }

  private final class RevisionAuthorMagicPredicate extends RevisionsSubjectMagicPredicate {
    RevisionAuthorMagicPredicate() {
      super(Vocabulary.SCHEMA_AUTHOR);
    }

    CloseableIteration<Statement, QueryEvaluationException> getStatements(NumericValueFactory.RevisionIRI subj, Value obj) {
      if (subj == null) {
        if (obj == null) {
          return revisionContributorIndex.longPrefixIteration(new long[]{}, (key, value) -> valueFactory.createStatement(
                  valueFactory.createRevisionIRI(key),
                  getPredicate(),
                  valueFactory.createLiteral(value)
          ));
        } else {
          return contributorRevisionsIndex.stringPrefixIteration(obj.stringValue(), (key, value) -> valueFactory.createStatement(
                  valueFactory.createRevisionIRI(key.getValue()),
                  getPredicate(),
                  obj
          ));
        }
      } else {
        if (obj == null) {
          String contributor = revisionContributorIndex.get(subj.getRevisionId());
          return contributor == null ? EMPTY_ITERATION : toIteration(subj, getPredicate(), valueFactory.createLiteral(contributor));
        } else {
          return obj.stringValue().equals(revisionContributorIndex.get(subj.getRevisionId()))
                  ? toIteration(subj, getPredicate(), obj)
                  : EMPTY_ITERATION;
        }
      }
    }
  }

  private final class RevisionGlobalStateAtMagicPredicate extends RevisionsSubjectMagicPredicate {

    RevisionGlobalStateAtMagicPredicate() {
      super(Vocabulary.HISTORY_GLOBAL_STATE_AT);
    }

    @Override
    CloseableIteration<Statement, QueryEvaluationException> getStatements(NumericValueFactory.RevisionIRI subjRevision, Value obj) {
      if (!(obj instanceof Literal && ((Literal) obj).getDatatype().equals(XMLSchema.DATETIME))) {
        return EMPTY_ITERATION;
      }

      try {
        long date = Instant.parse(obj.stringValue()).getEpochSecond();
        CloseableIteration<long[], QueryEvaluationException> iter = dateRevisionsIndex.longPrefixIteration(date, (d, rev) -> rev);
        if (!iter.hasNext()) {
          return EMPTY_ITERATION;
        }
        long[] revisions = iter.next();
        Arrays.sort(revisions);
        NumericValueFactory.RevisionIRI revision = valueFactory.createRevisionIRI(revisions[0], Vocabulary.SnapshotType.GLOBAL_STATE);

        return (subjRevision == null || subjRevision.equals(revision))
                ? toIteration(revision, getPredicate(), obj)
                : EMPTY_ITERATION;
      } catch (DateTimeParseException e) {
        throw new QueryEvaluationException(obj + " is an invalid revision timestamp");
      }
    }
  }
}
