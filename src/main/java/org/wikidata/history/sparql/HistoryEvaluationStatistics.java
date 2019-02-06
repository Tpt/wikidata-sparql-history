package org.wikidata.history.sparql;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;

import java.util.Arrays;
import java.util.List;

public class HistoryEvaluationStatistics extends EvaluationStatistics {

  private static final List<Value> REVISION_ATTRIBUTES = Arrays.asList(
          Vocabulary.HISTORY_GLOBAL_STATE,
          Vocabulary.HISTORY_ADDITIONS,
          Vocabulary.HISTORY_DELETIONS,
          Vocabulary.HISTORY_REVISION_ID,
          Vocabulary.HISTORY_PREVIOUS_REVISION,
          Vocabulary.HISTORY_NEXT_REVISION,
          Vocabulary.SCHEMA_DATE_CREATED,
          Vocabulary.SCHEMA_IS_BASED_ON
  );

  @Override
  protected CardinalityCalculator createCardinalityCalculator() {
    return new HistoryCardinalityCalculator();
  }

  protected class HistoryCardinalityCalculator extends CardinalityCalculator {

    @Override
    public double getCardinality(StatementPattern sp) {
      Value context = (sp.getContextVar() == null) ? null : sp.getContextVar().getValue();

      if (context != null) {
        if (!(context instanceof IRI)) {
          return 0;
        }
        String contextNamespace = ((IRI) context).getNamespace();
        switch (contextNamespace) {
          case Vocabulary.REVISION_ADDITIONS_NAMESPACE:
          case Vocabulary.REVISION_DELETIONS_NAMESPACE:
            return 1; // The number of triple is always small, let's use it first
          case Vocabulary.REVISION_GLOBAL_STATE_NAMESPACE:
            return getDataTripleCardinality(sp.getSubjectVar().getValue(), sp.getPredicateVar().getValue(), sp.getObjectVar().getValue());
          default:
            return 0; // Does not exists
        }
      }

      Value subject = sp.getSubjectVar().getValue();
      Value predicate = sp.getPredicateVar().getValue();
      Value object = sp.getObjectVar().getValue();

      if (predicate != null && sp.getContextVar() == null) {
        if (REVISION_ATTRIBUTES.contains(predicate)) {
          return (subject != null || object != null) ? 1 : Integer.MAX_VALUE;
        }
        if (predicate.equals(Vocabulary.SCHEMA_ABOUT)) {
          if (subject != null) {
            return 1;
          } else if (object != null) {
            return 100;
          } else {
            return Integer.MAX_VALUE;
          }
        }
        if (predicate.equals(Vocabulary.SCHEMA_AUTHOR)) {
          if (subject != null) {
            return 1;
          } else if (object != null) {
            return 10000;
          } else {
            return Integer.MAX_VALUE;
          }
        }
      }

      //We are querying revision data without predicate
      if (
              (subject instanceof IRI && Vocabulary.REVISION_NAMESPACE.equals(((IRI) subject).getNamespace())) ||
                      (object instanceof IRI && Vocabulary.REVISION_NAMESPACE.equals(((IRI) object).getNamespace()))
      ) {
        return 10;
      }

      return getDataTripleCardinality(subject, predicate, object);
    }

    private double getDataTripleCardinality(Value subject, Value predicate, Value object) {
      //TODO: improve with statistics
      if (subject != null) {
        return (predicate != null || object != null) ? 1 : 100;
      }
      if (object != null) {
        return 100_000;
      }
      return 1_000_000_000;
    }
  }
}
