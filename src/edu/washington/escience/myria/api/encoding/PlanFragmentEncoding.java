package edu.washington.escience.myria.api.encoding;

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * A JSON-able wrapper for the expected wire message for a query.
 */
public final class PlanFragmentEncoding extends MyriaApiEncoding {
  /** Which workers execute this plan. */
  public List<Integer> workers;
  /** The Operators in this plan fragment. */
  public List<OperatorEncoding<?>> operators;
  /** List of required fields. */
  public static final List<String> requiredFields = ImmutableList.of("operators");

  @Override
  protected List<String> getRequiredFields() {
    return requiredFields;
  }

  @Override
  protected void validateExtra() {
    for (OperatorEncoding<?> op : operators) {
      op.validate();
    }
  }
}