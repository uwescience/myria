package edu.washington.escience.myria.api.encoding;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.ws.rs.core.Response.Status;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.operator.Operator;

/**
 * A JSON-able wrapper for the expected wire message for a query.
 */
public final class PlanFragmentEncoding extends MyriaApiEncoding {
  /** Which workers execute this plan. */
  public List<Integer> workers;
  /** Override the workers that will execute this fragment. */
  public List<Integer> overrideWorkers;
  /** The Operators in this plan fragment. */
  @Required public List<OperatorEncoding<? extends Operator>> operators;
  /** Index of this fragment. */
  public int fragmentIndex = -1;
  /** List of required fields. */
  public static final List<String> requiredFields = ImmutableList.of("operators");

  /**
   * Construct a PlanFragmentEncoding wrapping the given operators
   *
   * @param operators
   */
  @SafeVarargs
  public static PlanFragmentEncoding of(final OperatorEncoding<? extends Operator>... operators) {
    PlanFragmentEncoding ret = new PlanFragmentEncoding();
    ret.operators = Arrays.asList(operators);
    return ret;
  }

  public void setFragmentIndex(final int fragmentIndex) {
    this.fragmentIndex = fragmentIndex;
  }

  @Override
  protected void validateExtra() {
    Set<Integer> opNames = new HashSet<Integer>();
    for (OperatorEncoding<? extends Operator> op : operators) {
      op.validate();
      if (!opNames.contains(op.opId)) {
        opNames.add(op.opId);
      } else {
        throw new MyriaApiException(
            Status.BAD_REQUEST,
            "duplicate operator IDs in a fragment: "
                + op.opId
                + ". Found on operator "
                + op.opName
                + " of type "
                + op.getClass());
      }
    }
  }
}
