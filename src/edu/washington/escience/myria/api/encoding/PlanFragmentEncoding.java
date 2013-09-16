package edu.washington.escience.myria.api.encoding;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.ws.rs.core.Response.Status;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.api.MyriaApiException;

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
    Set<String> opNames = new HashSet<String>();
    for (OperatorEncoding<?> op : operators) {
      op.validate();
      if (!opNames.contains(op.opName)) {
        opNames.add(op.opName);
      } else {
        throw new MyriaApiException(Status.BAD_REQUEST, "duplicate operator names in a fragment: " + op.opName);
      }
    }
  }
}