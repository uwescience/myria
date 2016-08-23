package edu.washington.escience.myria.api.encoding;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PerfEnforceQueryMetadataEncoding {
  public int id;
  public double slaRuntime;
  public String description;

  public PerfEnforceQueryMetadataEncoding() {}

  @JsonCreator
  public PerfEnforceQueryMetadataEncoding(
      @JsonProperty(value = "id", required = true) final int id,
      @JsonProperty(value = "slaRuntime", required = true) final double slaRuntime) {
    this.id = id;
    this.slaRuntime = slaRuntime;
  }

  public double getSLA() {
    return slaRuntime;
  }
}
