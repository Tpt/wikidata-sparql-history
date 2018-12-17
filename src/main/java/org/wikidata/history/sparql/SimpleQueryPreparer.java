package org.wikidata.history.sparql;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UpdateExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.AbstractQueryPreparer;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.*;
import org.eclipse.rdf4j.repository.sparql.federation.SPARQLServiceResolver;

public final class SimpleQueryPreparer extends AbstractQueryPreparer {
  private static final QueryOptimizer[] SIMPLE_OPTIMIZERS = new QueryOptimizer[]{
          new PropertyPathOptimizer(),
          new KnownClosureOptimizer(),
          new BindingAssigner(),
          new CompareOptimizer(),
          new ConjunctiveConstraintSplitter(),
          new DisjunctiveConstraintOptimizer(),
          new SameTermFilterOptimizer(),
          new QueryModelNormalizer(),
          new IterativeEvaluationOptimizer(),
          new FilterOptimizer(),
          new OrderLimitOptimizer(),
  };

  public SimpleQueryPreparer(TripleSource tripleSource) {
    super(tripleSource);
  }

  @Override
  protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(
          TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred, int maxExecutionTime
  ) throws QueryEvaluationException {
    tupleExpr = tupleExpr.clone();
    if (!(tupleExpr instanceof QueryRoot)) {
      tupleExpr = new QueryRoot(tupleExpr);
    }

    EvaluationStrategy strategy = new ExtendedEvaluationStrategy(
            getTripleSource(), dataset, new SPARQLServiceResolver(), 0L
    );

    for (QueryOptimizer optimizer : SIMPLE_OPTIMIZERS) {
      optimizer.optimize(tupleExpr, dataset, bindings);
    }
    new ConstantOptimizer(strategy).optimize(tupleExpr, dataset, bindings);

    new ExprValueConverter(getTripleSource().getValueFactory()).optimize(tupleExpr, dataset, bindings);

    //System.out.println("Query plan:\n" + tupleExpr);

    return strategy.evaluate(tupleExpr, bindings);
  }

  @Override
  protected void execute(
          UpdateExpr updateExpr, Dataset dataset, BindingSet bindings, boolean includeInferred, int maxExecutionTime
  ) throws UpdateExecutionException {
    throw new UpdateExecutionException("This repository is read only");
  }
}