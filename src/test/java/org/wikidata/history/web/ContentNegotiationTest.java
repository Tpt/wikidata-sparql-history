package org.wikidata.history.web;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

class ContentNegotiationTest {

  @Test
  void testNegotiateAccept() {
    testAcceptNegotiation(null, Optional.of("application/ld+json"));
    testAcceptNegotiation("application/ld+json", Optional.of("application/ld+json"));
    testAcceptNegotiation("application/*", Optional.of("application/ld+json"));
    testAcceptNegotiation("*/*", Optional.of("application/ld+json"));
    testAcceptNegotiation("application/json", Optional.of("application/json"));
    testAcceptNegotiation("application/ld+json; charset=UTF-8", Optional.of("application/ld+json"));
    testAcceptNegotiation("application/*; charset=UTF-8", Optional.of("application/ld+json"));
    testAcceptNegotiation("*/*; charset=UTF-8", Optional.of("application/ld+json"));
    testAcceptNegotiation("application/json; charset=UTF-8", Optional.of("application/json"));
    testAcceptNegotiation("application/xml", Optional.empty());

  }

  private void testAcceptNegotiation(String header, Optional<String> expected) {
    List<String> possibles = Arrays.asList("application/ld+json", "application/json");
    Assertions.assertEquals(expected, ContentNegotiation.negotiateAccept(header, possibles));
  }
}

