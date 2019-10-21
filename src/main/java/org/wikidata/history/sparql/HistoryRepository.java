package org.wikidata.history.sparql;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.ExceptionConvertingIteration;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryPreparer;
import org.eclipse.rdf4j.query.parser.*;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.base.AbstractRepository;
import org.eclipse.rdf4j.repository.base.AbstractRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.eclipse.rdf4j.query.QueryLanguage.SPARQL;

public class HistoryRepository extends AbstractRepository implements AutoCloseable {

  private Path indexPath;
  private RocksTripleSource tripleSource;
  private QueryParser queryParser;
  private QueryPreparer queryPreparer;

  public HistoryRepository() {
  }

  public HistoryRepository(Path indexPath) {
    this.indexPath = indexPath;
    init();
  }

  @Override
  protected void initializeInternal() throws RepositoryException {
    tripleSource = new RocksTripleSource(indexPath);
    queryParser = new SPARQLParser();
    queryPreparer = new SimpleQueryPreparer(tripleSource);
  }

  @Override
  protected void shutDownInternal() throws RepositoryException {
    tripleSource.close();
  }

  @Override
  public void setDataDir(File file) {
    indexPath = Paths.get(file.toURI());
  }

  @Override
  public File getDataDir() {
    return indexPath.toFile();
  }

  @Override
  public boolean isWritable() throws RepositoryException {
    return false;
  }

  @Override
  public RepositoryConnection getConnection() throws RepositoryException {
    return new HistoryConnection(this);
  }

  @Override
  public ValueFactory getValueFactory() {
    return tripleSource.getValueFactory();
  }

  @Override
  public void close() {
    shutDownInternal();
  }

  private static final class HistoryConnection extends AbstractRepositoryConnection {

    private HistoryRepository repository;

    private HistoryConnection(HistoryRepository repository) {
      super(repository);
      this.repository = repository;
    }

    @Override
    protected void addWithoutCommit(Resource resource, IRI iri, Value value, Resource... resources) throws RepositoryException {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void removeWithoutCommit(Resource resource, IRI iri, Value value, Resource... resources) throws RepositoryException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Query prepareQuery(QueryLanguage ql, String query, String base) throws RepositoryException, MalformedQueryException {
      if (QueryLanguage.SPARQL.equals(ql)) {
        String strippedQuery = QueryParserUtil.removeSPARQLQueryProlog(query).toUpperCase();
        if (strippedQuery.startsWith("SELECT")) {
          return prepareTupleQuery(ql, query, base);
        } else if (strippedQuery.startsWith("ASK")) {
          return prepareBooleanQuery(ql, query, base);
        } else {
          return prepareGraphQuery(ql, query, base);
        }
      } else {
        throw new UnsupportedOperationException("Unsupported query language " + ql);
      }
    }

    @Override
    public TupleQuery prepareTupleQuery(QueryLanguage ql, String query, String base) throws RepositoryException, MalformedQueryException {
      ParsedQuery parsedQuery = parseQuery(ql, query, base);
      if (parsedQuery instanceof ParsedTupleQuery) {
        return repository.queryPreparer.prepare((ParsedTupleQuery) parsedQuery);
      } else {
        throw new MalformedQueryException("Not supported query: " + parsedQuery.toString());
      }
    }

    @Override
    public GraphQuery prepareGraphQuery(QueryLanguage ql, String query, String base) throws RepositoryException, MalformedQueryException {
      ParsedQuery parsedQuery = parseQuery(ql, query, base);
      if (parsedQuery instanceof ParsedGraphQuery) {
        return repository.queryPreparer.prepare((ParsedGraphQuery) parsedQuery);
      } else {
        throw new MalformedQueryException("Not supported query: " + parsedQuery.toString());
      }
    }

    @Override
    public BooleanQuery prepareBooleanQuery(QueryLanguage ql, String query, String base) throws RepositoryException, MalformedQueryException {
      ParsedQuery parsedQuery = parseQuery(ql, query, base);
      if (parsedQuery instanceof ParsedBooleanQuery) {
        return repository.queryPreparer.prepare((ParsedBooleanQuery) parsedQuery);
      } else {
        throw new MalformedQueryException("Not supported query: " + parsedQuery.toString());
      }
    }

    private ParsedQuery parseQuery(QueryLanguage ql, String query, String base) {
      if (SPARQL.equals(ql)) {
        try {
          return repository.queryParser.parseQuery(query, base);
        } catch (MalformedQueryException e) {
          throw new MalformedQueryException(e.getMessage() + "\nQuery:\n" + query, e);
        }
      } else {
        throw new UnsupportedQueryLanguageException("Unsupported query language " + ql);
      }
    }

    @Override
    public Update prepareUpdate(QueryLanguage queryLanguage, String s, String s1) throws RepositoryException, MalformedQueryException {
      throw new UnsupportedOperationException();
    }

    @Override
    public RepositoryResult<Resource> getContextIDs() throws RepositoryException {
      throw new UnsupportedOperationException();
    }

    @Override
    public RepositoryResult<Statement> getStatements(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) throws RepositoryException {
      return new RepositoryResult<>(new ExceptionConvertingIteration<Statement, RepositoryException>(
              repository.tripleSource.getStatements(subj, pred, obj, contexts)
      ) {
        @Override
        protected RepositoryException convert(Exception e) {
          return new RepositoryException(e);
        }
      });
    }

    @Override
    public boolean hasStatement(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) throws RepositoryException {
      try (CloseableIteration<Statement, QueryEvaluationException> statements = repository.tripleSource.getStatements(subj, pred, obj, contexts)) {
        return statements.hasNext();
      } catch (QueryEvaluationException e) {
        throw new RepositoryException(e);
      }
    }

    @Override
    public void exportStatements(Resource subj, IRI pred, Value obj, boolean includeInferred, RDFHandler handler, Resource... contexts) throws RepositoryException, RDFHandlerException {
    }

    @Override
    public long size(Resource... contexts) throws RepositoryException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isActive() throws RepositoryException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void begin() throws RepositoryException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void commit() throws RepositoryException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void rollback() throws RepositoryException {
      throw new UnsupportedOperationException();
    }

    @Override
    public RepositoryResult<Namespace> getNamespaces() throws RepositoryException {
      return new RepositoryResult<>(new EmptyIteration<>());
    }

    @Override
    public String getNamespace(String prefix) throws RepositoryException {
      return null;
    }

    @Override
    public void setNamespace(String prefix, String name) throws RepositoryException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void removeNamespace(String prefix) throws RepositoryException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clearNamespaces() throws RepositoryException {
      throw new UnsupportedOperationException();
    }
  }
}
