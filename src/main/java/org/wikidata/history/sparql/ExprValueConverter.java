package org.wikidata.history.sparql;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

final class ExprValueConverter implements QueryOptimizer {

  private final ValueFactory valueFactory;

  ExprValueConverter(ValueFactory valueFactory) {
    this.valueFactory = valueFactory;
  }

  public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
    if (valueFactory instanceof NumericValueFactory) {
      tupleExpr.visit(new ClosureVisitor((NumericValueFactory) valueFactory));
    }
  }

  protected static class ClosureVisitor extends AbstractQueryModelVisitor<RuntimeException> {

    private final NumericValueFactory valueFactory;

    ClosureVisitor(NumericValueFactory valueFactory) {
      this.valueFactory = valueFactory;
    }

    @Override
    public void meet(Var var) throws RuntimeException {
      Value value = var.getValue();
      if (value instanceof IRI) {
        var.setValue(valueFactory.createIRI((IRI) value));
      } else if (value instanceof BNode) {
        var.setValue(valueFactory.createBNode((BNode) value));
      } else if (value instanceof Literal) {
        var.setValue(valueFactory.createLiteral((Literal) value));
      }
    }
  }
}
