package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.base.Verify;

import edu.washington.escience.myria.MyriaConstants.FTMODE;
import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.api.encoding.meta.FragmentEncoding;
import edu.washington.escience.myria.api.encoding.meta.MetaTaskEncoding;

/**
 * A JSON-able wrapper for the expected wire message for a query.
 * 
 */
public class QueryEncoding extends MyriaApiEncoding {
  /** The raw Datalog. */
  @Required
  public String rawDatalog;
  /** The logical relation algebra plan. */
  @Required
  public String logicalRa;
  /** Set whether this query is run in profiling mode. (default is false) */
  public boolean profilingMode = false;
  /** The fault-tolerance mode used in this query, default: none. */
  public FTMODE ftMode = FTMODE.none;

  /** The old physical query plan encoding. */
  public List<PlanFragmentEncoding> fragments;
  /** The new meta task query plan encoding. */
  public MetaTaskEncoding plan;

  @Override
  protected void validateExtra() throws MyriaApiException {
    Preconditions.checkArgument((fragments == null) ^ (plan == null),
        "exactly one of fragments or plan must be specified");
    /* If they gave us an old plan type, convert it to a new plan type. */
    if (fragments != null) {
      plan = new FragmentEncoding(fragments);
      fragments = null;
    }
    plan.validate();
  }

  public Set<Integer> getWorkers() {
    Verify.verify(fragments == null, "fragments should be null. Was this QueryEncoding validated?");
    return plan.getWorkers();
  }
}