package edu.washington.escience.myria.util;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enumeration of supported sampling types.
 */
public enum SamplingType {
  /** with replacement */
  WithReplacement("WR"),
  /** without replacement */
  WithoutReplacement("WoR");

  /** The short name of this sampling type. */
  private String shortName;

  /**
   * 
   * @param shortName short name.
   */
  SamplingType(final String shortName) {
    this.shortName = shortName;
  }

  /**
   * 
   * @return the short name.
   */
  @JsonValue
  public String getShortName() {
    return shortName;
  }
}
