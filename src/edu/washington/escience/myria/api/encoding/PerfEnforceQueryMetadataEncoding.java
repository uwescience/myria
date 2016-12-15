package edu.washington.escience.myria.api.encoding;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

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
  }

  public double getSLA() {
    return slaRuntime;
  }

  public String getQueryText() {
    return queryText;
  }
}
