package org.wikidata.history.web;

import io.javalin.BadRequestResponse;
import io.javalin.Context;
import io.javalin.HttpResponseException;
import io.javalin.InternalServerErrorResponse;
import io.javalin.core.util.Header;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.eclipse.rdf4j.common.lang.FileFormat;
import org.eclipse.rdf4j.common.lang.service.FileFormatServiceRegistry;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryPreparer;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.parser.*;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.query.resultio.BooleanQueryResultWriterFactory;
import org.eclipse.rdf4j.query.resultio.BooleanQueryResultWriterRegistry;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriterFactory;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriterRegistry;
import org.eclipse.rdf4j.rio.RDFWriterFactory;
import org.eclipse.rdf4j.rio.RDFWriterRegistry;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.history.sparql.SimpleQueryPreparer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

class SparqlEndpoint {
  private static final int QUERY_TIMOUT_IN_S = 60 * 5;
  private static final Logger LOGGER = LoggerFactory.getLogger(SparqlEndpoint.class);

  private final QueryParser queryParser = new SPARQLParser();
  private final QueryPreparer queryPreparer;
  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private final QueryLogger queryLogger;

  SparqlEndpoint(TripleSource tripleSource, QueryLogger queryLogger) {
    queryPreparer = new SimpleQueryPreparer(tripleSource);
    this.queryLogger = queryLogger;
  }

  void get(Context context) {
    String query = context.queryParam("query");
    if (query == null) {
      executeDescription(context);
    } else {
      executeQuery(query, context);
    }
  }

  void post(Context context) {
    String contentType = context.contentType();
    if (contentType != null) {
      contentType = contentType.split(";")[0].trim();
    }
    if ("application/x-www-form-urlencoded".equals(contentType)) {
      executeQuery(URLEncodedUtils.parse(context.body(), StandardCharsets.UTF_8).stream()
                      .filter(t -> t.getName().trim().equals("query"))
                      .map(NameValuePair::getValue)
                      .findAny()
                      .orElseThrow(() -> new BadRequestResponse("The 'query' urlencoded parameter is mandatory")),
              context);
    } else if ("application/sparql-query".equals(contentType)) {
      executeQuery(context.body(), context);
    } else {
      throw new BadRequestResponse("Unexpected Content-Type: " + contentType);
    }
  }

  private void executeDescription(Context context) {
    outputWithFormat(RDFWriterRegistry.getInstance(), context, (service, outputStream) ->
            Rio.write(getServiceDescription(), service.getWriter(outputStream))
    );
  }

  private void executeQuery(String query, Context context) {
    ParsedQuery parsedQuery;
    try {
      parsedQuery = queryParser.parseQuery(query, null);
    } catch (MalformedQueryException e) {
      throw new BadRequestResponse(e.getMessage());
    }
    queryLogger.logQuery(parsedQuery.getSourceString());
    if (parsedQuery instanceof ParsedBooleanQuery) {
      evaluateBooleanQuery((ParsedBooleanQuery) parsedQuery, context);
    } else if (parsedQuery instanceof ParsedGraphQuery) {
      evaluateGraphQuery((ParsedGraphQuery) parsedQuery, context);
    } else if (parsedQuery instanceof ParsedTupleQuery) {
      evaluateTupleQuery((ParsedTupleQuery) parsedQuery, context);
    } else {
      throw new BadRequestResponse("Unsupported kind of query: " + parsedQuery.toString());
    }

  }

  private void evaluateBooleanQuery(ParsedBooleanQuery parsedQuery, Context context) {
    outputWithFormat(BooleanQueryResultWriterRegistry.getInstance(), context, (service, outputStream) -> {
              try {
                BooleanQuery query = queryPreparer.prepare(parsedQuery);
                query.setMaxExecutionTime(QUERY_TIMOUT_IN_S);
                service.getWriter(outputStream).handleBoolean(query.evaluate());
              } catch (QueryEvaluationException e) {
                LOGGER.info(e.getMessage(), e);
                throw new InternalServerErrorResponse(e.getMessage());
              }
            }
    );
  }

  private void evaluateGraphQuery(ParsedGraphQuery parsedQuery, Context context) {
    outputWithFormat(RDFWriterRegistry.getInstance(), context, (service, outputStream) -> {
      try {
        GraphQuery query = queryPreparer.prepare(parsedQuery);
        query.setMaxExecutionTime(QUERY_TIMOUT_IN_S);
        query.evaluate(service.getWriter(outputStream));
      } catch (QueryEvaluationException e) {
        LOGGER.info(e.getMessage(), e);
        throw new InternalServerErrorResponse(e.getMessage());
      }
    });
  }

  private void evaluateTupleQuery(ParsedTupleQuery parsedQuery, Context context) {
    outputWithFormat(TupleQueryResultWriterRegistry.getInstance(), context, (service, outputStream) -> {
      try {
        TupleQuery query = queryPreparer.prepare(parsedQuery);
        query.setMaxExecutionTime(QUERY_TIMOUT_IN_S);
        query.evaluate(service.getWriter(outputStream));
      } catch (QueryEvaluationException e) {
        LOGGER.info(e.getMessage(), e);
        throw new InternalServerErrorResponse(e.getMessage());
      }
    });
  }

  private <FF extends FileFormat, S> void outputWithFormat(FileFormatServiceRegistry<FF, S> writerRegistry, Context context, BiConsumer<S, OutputStream> addToOutput) {
    List<String> accepted = writerRegistry.getKeys().stream().flatMap(k -> k.getMIMETypes().stream()).collect(Collectors.toList());
    String mimeType;
    try {
      mimeType = ContentNegotiation.negotiateAccept(context.header(Header.ACCEPT), accepted)
              .orElseThrow(() -> new NotAcceptableResponse("No acceptable result format found. Accepted format are: " + accepted.toString()));
    } catch (IllegalArgumentException e) {
      throw new BadRequestResponse(e.getMessage());
    }

    FF fileFormat = writerRegistry.getFileFormatForMIMEType(mimeType).orElseThrow(() -> {
      LOGGER.error("Not able to retrieve writer for " + mimeType);
      return new InternalServerErrorResponse("Not able to retrieve writer for " + mimeType);
    });
    S service = writerRegistry.get(fileFormat).orElseThrow(() -> {
      LOGGER.error("Unable to write " + fileFormat);
      return new InternalServerErrorResponse("Unable to write " + fileFormat);
    });

    try {
      PipedOutputStream outputStream = new PipedOutputStream();
      PipedInputStream inputStream = new PipedInputStream(outputStream);
      context.contentType(mimeType);
      context.result(inputStream);
      executorService.submit(() -> {
        try {
          addToOutput.accept(service, outputStream);
        } catch (HttpResponseException e) {
          try {
            context.status(e.getStatus());
            context.contentType("text/plain");
            outputStream.write(e.getMessage().getBytes());
          } catch (IOException e1) {
            LOGGER.error(e.getMessage(), e);
          }
        } finally {
          try {
            outputStream.close();
          } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
          }
        }
      });
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
      throw new InternalServerErrorResponse();
    }
  }

  private Model getServiceDescription() {
    ValueFactory valueFactory = SimpleValueFactory.getInstance();
    Model model = new TreeModel();

    Resource service = valueFactory.createBNode();
    model.add(service, RDF.TYPE, SD.SERVICE);
    //TODO model.add(service, SD.ENDPOINT, )
    model.add(service, SD.FEATURE_PROPERTY, SD.BASIC_FEDERATED_QUERY);
    model.add(service, SD.SUPPORTED_LANGUAGE, SD.SPARQL_10_QUERY);
    model.add(service, SD.SUPPORTED_LANGUAGE, SD.SPARQL_11_QUERY);

    for (TupleQueryResultWriterFactory queryResultWriterFactory : TupleQueryResultWriterRegistry.getInstance().getAll()) {
      Resource formatIRI = queryResultWriterFactory.getTupleQueryResultFormat().getStandardURI();
      if (formatIRI != null) {
        model.add(service, SD.RESULT_FORMAT, formatIRI);
      }
    }
    for (BooleanQueryResultWriterFactory queryResultWriterFactory : BooleanQueryResultWriterRegistry.getInstance().getAll()) {
      Resource formatIRI = queryResultWriterFactory.getBooleanQueryResultFormat().getStandardURI();
      if (formatIRI != null) {
        model.add(service, SD.RESULT_FORMAT, formatIRI);
      }
    }
    for (RDFWriterFactory formatWriterFactory : RDFWriterRegistry.getInstance().getAll()) {
      Resource formatIRI = formatWriterFactory.getRDFFormat().getStandardURI();
      if (formatIRI != null) {
        model.add(service, SD.RESULT_FORMAT, formatIRI);
      }
    }

    return model;
  }
}
