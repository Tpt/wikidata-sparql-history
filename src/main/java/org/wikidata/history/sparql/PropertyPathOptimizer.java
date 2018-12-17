package org.wikidata.history.sparql;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.ArbitraryLengthPath;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

import java.util.Objects;

/**
 * Improves the property paths. Currently rewrites:
 * - p / p* to p+
 * - p* / p to p+
 */
final class PropertyPathOptimizer implements QueryOptimizer {

  public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
    tupleExpr.visit(new ClosureVisitor());
  }

  protected static class ClosureVisitor extends AbstractQueryModelVisitor<RuntimeException> {

    @Override
    public void meet(Join join) {
      super.meet(join);

      TupleExpr left = join.getLeftArg();
      TupleExpr right = join.getRightArg();

      if (left instanceof StatementPattern && right instanceof ArbitraryLengthPath) {
        StatementPattern leftPattern = (StatementPattern) left;
        ArbitraryLengthPath rightClosure = (ArbitraryLengthPath) right;

        if (rightClosure.getPathExpression() instanceof StatementPattern) {
          StatementPattern rightPattern = (StatementPattern) rightClosure.getPathExpression();
          if (Objects.equals(leftPattern.getPredicateVar(), rightPattern.getPredicateVar()) &&
                  Objects.equals(leftPattern.getContextVar(), rightPattern.getContextVar()) &&
                  leftPattern.getScope() == rightPattern.getScope()) {
            if (leftPattern.getObjectVar().equals(rightPattern.getSubjectVar())) {
              join.replaceWith(new ArbitraryLengthPath(
                      rightClosure.getScope(),
                      leftPattern.getSubjectVar(),
                      new StatementPattern(
                              leftPattern.getScope(),
                              leftPattern.getSubjectVar(),
                              leftPattern.getPredicateVar(),
                              rightClosure.getObjectVar(),
                              rightClosure.getContextVar()
                      ),
                      rightClosure.getObjectVar(),
                      rightClosure.getContextVar(),
                      rightClosure.getMinLength() + 1
              ));
            }
          }
        }
      } else if (left instanceof ArbitraryLengthPath && right instanceof StatementPattern) {
        ArbitraryLengthPath leftClosure = (ArbitraryLengthPath) left;
        StatementPattern rightPattern = (StatementPattern) right;

        if (leftClosure.getPathExpression() instanceof StatementPattern) {
          StatementPattern leftPattern = (StatementPattern) leftClosure.getPathExpression();
          if (Objects.equals(leftPattern.getPredicateVar(), rightPattern.getPredicateVar()) &&
                  Objects.equals(leftPattern.getContextVar(), rightPattern.getContextVar()) &&
                  leftPattern.getScope() == rightPattern.getScope()) {
            if (leftPattern.getObjectVar().equals(rightPattern.getSubjectVar())) {
              join.replaceWith(new ArbitraryLengthPath(
                      leftClosure.getScope(),
                      leftPattern.getSubjectVar(),
                      new StatementPattern(
                              leftPattern.getScope(),
                              leftPattern.getSubjectVar(),
                              leftPattern.getPredicateVar(),
                              rightPattern.getObjectVar(),
                              rightPattern.getContextVar()
                      ),
                      rightPattern.getObjectVar(),
                      leftClosure.getContextVar(),
                      leftClosure.getMinLength() + 1
              ));
            }
          }
        }
      }
    }
  }
}
