package edu.washington.escience.myria.api.encoding;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Encodes the REST response when the user searches queries.
 */
public class QuerySearchResults {
  /* The maximum query id that matches the search. */
  @JsonProperty public long max;
  /* The minimum query id that matches the search. */
  @JsonProperty public long min;
  /* The actual search results. */
  @JsonProperty public List<QueryStatusEncoding> results;
}
