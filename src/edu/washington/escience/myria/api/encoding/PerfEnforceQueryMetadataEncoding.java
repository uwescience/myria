package edu.washington.escience.myria.api.encoding;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This class is used to encode information about a
 * query provided by the user. It returns a query identifier,
 * the SLA for the query, and the query SQL statement.
 *
 */
public class PerfEnforceQueryMetadataEncoding {
  public int id;
  public double slaRuntime;
  public String queryText;

  public PerfEnforceQueryMetadataEncoding() {}

  @JsonCreator
  public PerfEnforceQueryMetadataEncoding(
      @JsonProperty(value = "id", required = true) final int id,
      @JsonProperty(value = "slaRuntime", required = true) final double slaRuntime,
      @JsonProperty(value = "queryText", required = true) final String queryText) {

    this.id = id;
    this.slaRuntime = slaRuntime;
    this.queryText = queryText;
  }

  public double getSLA() {
    return slaRuntime;
  }

  public String getQueryText() {
    return queryText;
  }
}
