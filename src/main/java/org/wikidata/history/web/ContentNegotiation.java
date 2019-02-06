package org.wikidata.history.web;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * TODO: does not support parameters (charset...)
 */
class ContentNegotiation {

  static Optional<String> negotiateAccept(String acceptHeader, List<String> available) throws IllegalArgumentException {
    if (acceptHeader == null) {
      acceptHeader = "*/*";
    }

    return Arrays.stream(acceptHeader.split(","))
            .map(MediaRange::parse)
            .sorted(Comparator.reverseOrder())
            .flatMap(r -> available.stream().filter(r::match))
            .findFirst();
  }

  private static final class MediaRange implements Comparable<MediaRange> {
    final String type;
    final String subType;
    final float q;

    private MediaRange(String type, String subType, float q) {
      this.type = type;
      this.subType = subType;
      this.q = q;
    }

    static MediaRange parse(String mediaRange) throws IllegalArgumentException {
      float q = 1;

      String[] parts = mediaRange.split(";");
      String[] mime = parts[0].split("/");
      if (mime.length != 2) {
        throw new IllegalArgumentException("Invalid mime type: " + parts[0]);
      }
      String type = mime[0].trim();
      String subType = mime[1].trim();
      for (int i = 1; i < parts.length; i++) {
        String[] parameterParts = parts[i].split("=");
        if (parameterParts.length != 2) {
          throw new IllegalArgumentException("Invalid parameter: " + parts[i]);
        }
        String name = parameterParts[0].trim();
        String value = parameterParts[1].trim();
        if ("q".equals(name)) {
          try {
            q = Float.parseFloat(value);
          } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("q parameter value should be a float, found " + value);
          }
          if (q < 0 || q > 1) {
            throw new IllegalArgumentException("q parameter value should be between 0 and 1, found " + value);
          }
        }
      }
      return new MediaRange(type, subType, q);
    }

    boolean match(String mime) {
      String[] parts = mime.split("/", 2);
      return type.equals("*") || (type.equals(parts[0])) && (subType.equals("*") || subType.equals(parts[1]));
    }

    @Override
    public int compareTo(MediaRange mediaRange) {
      return Float.compare(q, mediaRange.q);
    }

    @Override
    public String toString() {
      return type + '/' + subType + "; q=" + q;
    }
  }
}
