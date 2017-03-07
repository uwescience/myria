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
  /** The query id. */
  public int id;
  /** The query's SLA. */
  public double slaRuntime;
  /** The query's sql text */
  public String queryText;

  /**
   * Constructor for the PerfEnforceQueryMetadataEncoding
   */
  public PerfEnforceQueryMetadataEncoding() {}

  /**
   * Constructor for the PerfEnforceQueryMetadataEncoding
   * @param id the identifier for the query
   * @param slaRuntime the SLA runtime of the query
   * @param queryText the string representation of the query
   */
  @JsonCreator
  public PerfEnforceQueryMetadataEncoding(
      @JsonProperty(value = "id", required = true) final int id,
      @JsonProperty(value = "slaRuntime", required = true) final double slaRuntime,
      @JsonProperty(value = "queryText", required = true) final String queryText) {

    this.id = id;
    this.slaRuntime = slaRuntime;
    this.queryText = queryText;
  }

  /**
   * Returns the SLA of the query
   * @return the SLA of the query
   */
  public double getSLA() {
    return slaRuntime;
  }

  /**
   * Returns the string representation of the query
   * @return the string representation of the query
   */
  public String getQueryText() {
    return queryText;
  }
}
