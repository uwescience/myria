package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

/**
 * A JSON-able wrapper for the expected wire message for a query.
 * 
 */
public class QueryEncoding extends MyriaApiEncoding {
  /** The raw Datalog. */
  public String rawDatalog;
  /** The logical relation algebra plan. */
  public String logicalRa;
  /** The query plan encoding. */
  public Map<Integer, List<List<OperatorEncoding<?>>>> queryPlan;
  /** The expected number of results (for testing). */
  public Long expectedResultSize;
  /** The list of required fields. */
  private static List<String> requiredFields = ImmutableList.of("rawDatalog", "logicalRa", "queryPlan");

  @Override
  protected List<String> getRequiredFields() {
    return requiredFields;
  }

  @Override
  protected void validateExtra() {
    for (final List<List<OperatorEncoding<?>>> fragment : queryPlan.values()) {
      for (final List<OperatorEncoding<?>> operators : fragment) {
        for (final OperatorEncoding<?> operator : operators) {
          operator.validate();
        }
      }
    }
  }
}