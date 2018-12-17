package org.wikidata.history.sparql;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.TupleExprs;

import java.util.HashMap;
import java.util.Map;

/**
 * Improves closure computation based on hardcoded closure queries
 */
final class KnownClosureOptimizer implements QueryOptimizer {

  private static final Map<IRI, IRI> KNOWN_CLOSURES = new HashMap<>();

  static {
    KNOWN_CLOSURES.put(
            SimpleValueFactory.getInstance().createIRI(Vocabulary.WDT_NAMESPACE, "P279"),
            Vocabulary.P279_CLOSURE
    );
  }

  public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
    tupleExpr.visit(new ClosureVisitor());
  }

  protected static class ClosureVisitor extends AbstractQueryModelVisitor<RuntimeException> {

    @Override
    public void meet(ArbitraryLengthPath path) {
      super.meet(path);

      TupleExpr pathExpression = path.getPathExpression();
      if (pathExpression instanceof StatementPattern) {
        StatementPattern innerPattern = (StatementPattern) pathExpression;
        if (innerPattern.getPredicateVar().hasValue() && KNOWN_CLOSURES.containsKey(innerPattern.getPredicateVar().getValue())) {
          StatementPattern closureStatementPattern = new StatementPattern(
                  innerPattern.getScope(),
                  innerPattern.getSubjectVar(),
                  TupleExprs.createConstVar(KNOWN_CLOSURES.get(innerPattern.getPredicateVar().getValue())),
                  innerPattern.getObjectVar(),
                  innerPattern.getContextVar()
          );
          if (path.getMinLength() == 0) {
            path.replaceWith(new Union(closureStatementPattern, new ZeroLengthPath(
                    innerPattern.getScope(),
                    innerPattern.getSubjectVar(),
                    innerPattern.getObjectVar(),
                    innerPattern.getContextVar()
            )));
          } else if (path.getMinLength() == 1) {
            path.replaceWith(closureStatementPattern);
          }
        }
      }
    }
  }
}