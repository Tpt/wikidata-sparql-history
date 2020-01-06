package org.wikidata.history.preprocessor;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.history.sparql.Vocabulary;
import org.wikidata.wdtk.datamodel.helpers.Datamodel;
import org.wikidata.wdtk.datamodel.helpers.DatamodelMapper;
import org.wikidata.wdtk.datamodel.implementation.EntityDocumentImpl;
import org.wikidata.wdtk.datamodel.interfaces.Sites;
import org.wikidata.wdtk.dumpfiles.*;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RevisionFileConverter {

  private static final Logger LOGGER = LoggerFactory.getLogger(RevisionFileConverter.class);
  private static final Pattern ENTITY_PAGE_TITLE_PATTERN = Pattern.compile("^(Item:|Property:|)([PQ]\\d+)$");
  private static final Pattern REDIRECTION_PATTERN = Pattern.compile("^\\{\"entity\":\"(.*)\",\"redirect\":\"(.*)\"}$");
  private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();
  private static final long[] EMPTY_LONG_ARRAY = new long[]{};

  private final HistoryOutput historyOutput;
  private final Sites sites;
  private final WikidataPropertyInformation propertyInformation;


  public RevisionFileConverter(HistoryOutput historyOutput) throws IOException {
    this.historyOutput = historyOutput;
    sites = (new DumpProcessingController("wikidatawiki")).getSitesInformation();
    propertyInformation = new WikidataPropertyInformation();
  }

  public void process(Path file) throws IOException, InterruptedException {
    MwDumpFileProcessor processor = new MwRevisionDumpFileProcessor(new RevisionProcessor(historyOutput, sites, propertyInformation));
    MwLocalDumpFile dumpFile = new MwLocalDumpFile(file.toString(), DumpContentType.FULL, null, null);
    for (int i = 0; i < 10; i++) {
      try {
        processor.processDumpFileContents(new BZip2CompressorInputStream(new BufferedInputStream(Files.newInputStream(file))), dumpFile);
        return;
      } catch (IOException e) {
        LOGGER.error(e.getMessage(), e);
        Thread.sleep(30000);
        if (i == 9) {
          throw e;
        }
      }
    }
  }

  private static final class RevisionProcessor implements MwRevisionProcessor {

    private final HistoryOutput historyOutput;
    private final Sites sites;
    private final WikidataPropertyInformation propertyInformation;
    private int currentPageId = -1;
    private final Map<Long, Set<Statement>> revisions = new TreeMap<>();
    private final Map<Statement, long[]> triplesHistory = new HashMap<>();
    private final ObjectReader entityReader = new DatamodelMapper(Datamodel.SITE_WIKIDATA)
            .enable(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT)
            .readerFor(EntityDocumentImpl.class);

    RevisionProcessor(HistoryOutput historyOutput, Sites sites, WikidataPropertyInformation propertyInformation) {
      this.historyOutput = historyOutput;
      this.sites = sites;
      this.propertyInformation = propertyInformation;
    }

    @Override
    public void startRevisionProcessing(String siteName, String baseUrl, Map<Integer, String> namespaces) {
    }

    @Override
    public void processRevision(MwRevision mwRevision) {
      try {
        int pageId = mwRevision.getPageId();
        long revisionId = mwRevision.getRevisionId();
        String text = mwRevision.getText();
        String entityId = getEntityIdFromPageTitle(mwRevision.getPrefixedTitle());

        if (entityId == null) {
          return; //Not a Wikibase entity
        }
        if (pageId != currentPageId) {
          processRevisions();
          currentPageId = pageId;
        }

        //Adds to revision history
        try {
          historyOutput.addRevision(revisionId, mwRevision.getParentRevisionId(), entityId, Instant.parse(mwRevision.getTimeStamp()), mwRevision.getContributor(), mwRevision.getComment());
        } catch (Exception e) {
          LOGGER.error(e.getMessage(), e);
        }

        //Redirection
        Matcher redirectionMatcher = REDIRECTION_PATTERN.matcher(text);
        if (redirectionMatcher.matches()) {
          revisions.put(revisionId, Collections.singleton(VALUE_FACTORY.createStatement(
                  VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, redirectionMatcher.group(1)),
                  OWL.SAMEAS,
                  VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, redirectionMatcher.group(2))
          )));
        } else {
          SetRdfOutput output = new SetRdfOutput();
          RdfBuilder converter = new RdfBuilder(output, sites, propertyInformation);
          converter.addEntityDocument(entityReader.readValue(text));
          revisions.put(revisionId, output.getStatements());
        }
      } catch (Exception e) {
        LOGGER.warn("Error while parsing revision " + mwRevision.toString() + ": " + e.getMessage());
      }
    }

    private void processRevisions() {
      long[] revisionIds = toSortedLongArrays(revisions.keySet());

      for (int i = 0; i < revisionIds.length; i++) {
        long revisionId = revisionIds[i];
        long nextRevisionId = (i + 1 == revisionIds.length) ? Long.MAX_VALUE : revisionIds[i + 1];
        if (nextRevisionId < revisionId) {
          LOGGER.error("The revision ids are not properly sorted.");
        }

        for (Statement statement : revisions.get(revisionId)) {
          long[] statementRevisions = triplesHistory.getOrDefault(statement, EMPTY_LONG_ARRAY);
          if (statementRevisions.length > 0 && statementRevisions[statementRevisions.length - 1] == revisionId) {
            statementRevisions[statementRevisions.length - 1] = nextRevisionId;
          } else {
            statementRevisions = Arrays.copyOf(statementRevisions, statementRevisions.length + 2);
            statementRevisions[statementRevisions.length - 2] = revisionId;
            statementRevisions[statementRevisions.length - 1] = nextRevisionId;
            triplesHistory.put(statement, statementRevisions);
          }
        }
      }

      for (Map.Entry<Statement, long[]> entry : triplesHistory.entrySet()) {
        try {
          if (!isSorted(entry.getValue())) {
            LOGGER.error("the revision ranges are not sorted: " + Arrays.toString(revisionIds));
          }
          historyOutput.addTriple(entry.getKey().getSubject(), entry.getKey().getPredicate(), entry.getKey().getObject(), entry.getValue());
        } catch (IOException e) {
          LOGGER.error(e.getMessage(), e);
        }
      }

      triplesHistory.clear();
      revisions.clear();
    }

    private long[] toSortedLongArrays(Set<Long> s) {
      long[] values = new long[s.size()];
      int i = 0;
      for (long v : s) {
        values[i] = v;
        i++;
      }
      Arrays.sort(values);
      return values;
    }

    private String getEntityIdFromPageTitle(String title) {
      Matcher matcher = ENTITY_PAGE_TITLE_PATTERN.matcher(title);
      return matcher.matches() ? matcher.group(2) : null;
    }

    @Override
    public void finishRevisionProcessing() {
      if (!revisions.isEmpty()) {
        processRevisions();
      }
    }

    private static boolean isSorted(long[] array) {
      for (int i = 1; i < array.length; i++) {
        if (array[i] <= array[i - 1]) {
          return false;
        }
      }
      return true;
    }
  }

  private static class SetRdfOutput implements RdfBuilder.RdfOutput {
    private final Set<Statement> statements = new HashSet<>();

    @Override
    public void outputStatement(Statement statement) {
      statements.add(statement);
    }

    Set<Statement> getStatements() {
      return statements;
    }
  }
}
