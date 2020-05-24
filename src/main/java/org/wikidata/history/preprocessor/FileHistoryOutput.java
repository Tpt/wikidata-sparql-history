package org.wikidata.history.preprocessor;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

public class FileHistoryOutput implements HistoryOutput {

  private final Writer revisionsWriter;
  private final Writer triplesWriter;


  public FileHistoryOutput(Path directory) throws IOException {
    revisionsWriter = gzipWriter(directory.resolve("revisions.tsv.gz"));
    triplesWriter = gzipWriter(directory.resolve("triples.tsv.gz"));
  }

  private Writer gzipWriter(Path path) throws IOException {
    return new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(Files.newOutputStream(path))));
  }

  public synchronized void addRevision(
          long revisionId, long parentRevisionId, String entityId, Instant timestamp, String contributorName, String comment
  ) throws IOException {
    revisionsWriter
            .append(Long.toString(revisionId)).append('\t')
            .append(Long.toString(parentRevisionId)).append('\t')
            .append(entityId).append('\t')
            .append(Long.toString(timestamp.getEpochSecond())).append('\t')
            .append(contributorName).append('\t')
            .append((comment == null) ? "" : NTriplesUtil.escapeString(comment)).append('\n');
  }

  public synchronized void addTriple(Resource subject, IRI predicate, Value object, long... revisionIds) throws IOException {
    triplesWriter.append(NTriplesUtil.toNTriplesString(subject)).append('\t')
            .append(NTriplesUtil.toNTriplesString(predicate)).append('\t')
            .append(NTriplesUtil.toNTriplesString(object)).append('\t')
            .append(Arrays.stream(revisionIds).mapToObj(Long::toString).collect(Collectors.joining(" "))).append("\n");
  }

  @Override
  public void close() throws IOException {
    revisionsWriter.close();
    triplesWriter.close();
  }
}
