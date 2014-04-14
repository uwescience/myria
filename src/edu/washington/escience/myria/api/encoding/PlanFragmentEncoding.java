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
  @Required
  public List<OperatorEncoding<?>> operators;
  /** Index of this fragment. */
  public long fragmentIndex = -1;
  /** List of required fields. */
  public static final List<String> requiredFields = ImmutableList.of("operators");

  public void setFragmentIndex(long fragmentIndex) {
    this.fragmentIndex = fragmentIndex;
  }

  @Override
  protected void validateExtra() {
    Set<String> opNames = new HashSet<String>();
    for (OperatorEncoding<?> op : operators) {
      op.validate();
      if (!opNames.contains(op.opID)) {
        opNames.add(op.opID);
      } else {
        throw new MyriaApiException(Status.BAD_REQUEST, "duplicate operator names in a fragment: " + op.opID);
      }
    }
  }
}