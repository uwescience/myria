package edu.washington.escience.myria.util;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enumeration of supported sampling types.
 */
public enum SamplingType {
  WithReplacement("WR"),
  WithoutReplacement("WoR");

  private String shortName;

  SamplingType(String shortName) {
    this.shortName = shortName;
  }

  @JsonValue
  public String getShortName() {
    return shortName;
  }
}
