package org.wikidata.history.web;

import io.javalin.http.HttpResponseException;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Map;

class NotAcceptableResponse extends HttpResponseException {

  NotAcceptableResponse(@NotNull String msg, @NotNull Map<String, String> details) {
    super(406, msg, details);
  }

  NotAcceptableResponse(@NotNull String msg) {
    this(msg, Collections.emptyMap());
  }
}
