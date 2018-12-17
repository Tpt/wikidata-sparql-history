package org.wikidata.history.sparql;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;

public class HistoryEvaluationStatistics extends EvaluationStatistics {

  @Override
  protected CardinalityCalculator createCardinalityCalculator() {
    return new HistoryCardinalityCalculator();
  }

  protected class HistoryCardinalityCalculator extends CardinalityCalculator {

    @Override
    public double getCardinality(StatementPattern sp) {
      Var subjet = sp.getSubjectVar();
      Var object = sp.getObjectVar();
      Var context = sp.getContextVar();

      if (!sp.getPredicateVar().hasValue()) {
        return Long.MAX_VALUE; //Not able to resolve such pattern -> maximal cost
      }
      Value predicate = sp.getPredicateVar().getValue();

      if (predicate.equals(Vocabulary.SCHEMA_ABOUT)) {
        if (subjet.hasValue()) {
          return 1;
        } else if (object.hasValue()) {
          return 100;
        } else {
          return Long.MAX_VALUE;
        }
      } else if (predicate.equals(Vocabulary.SCHEMA_DATE_CREATED) || predicate.equals(Vocabulary.SCHEMA_IS_BASED_ON)) {
        return subjet.hasValue() || object.hasValue() ? 1 : Long.MAX_VALUE;
      } else if (predicate.stringValue().startsWith(Vocabulary.WDT_NAMESPACE)) {
        //TODO: should we penalize more?
        double contextWeight = (context == null || !context.hasValue()) ? 100 : 1;
        if (subjet.hasValue()) {
          return contextWeight * (object.hasValue() ? 1 : 100);
        } else if (object.hasValue()) {
          return contextWeight * 100_000;
        } else {
          return contextWeight * 10_000_000;
        }
      } else {
        return Long.MAX_VALUE; //No good estimation
      }
    }
  }
}
