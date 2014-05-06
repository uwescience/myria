package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.api.MyriaApiException;

/**
 * A JSON-able wrapper for the expected wire message for a query.
 * 
 */
@JsonIgnoreProperties({ "expected_result" })
public class QueryEncoding extends MyriaApiEncoding {
  /** The raw Datalog. */
  @Required
  public String rawDatalog;
  /** The logical relation algebra plan. */
  @Required
  public String logicalRa;
  /** The query plan encoding. */
  @Required
  public List<PlanFragmentEncoding> fragments;
  /** Set whether this query is run in profiling mode. (default is false) */
  public boolean profilingMode = false;
  /** The expected number of results (for testing). */
  public Long expectedResultSize;
  /** The fault-tolerance mode used in this query, default: none. */
  public String ftMode = "none";

  @Override
  protected void validateExtra() throws MyriaApiException {
    for (final PlanFragmentEncoding fragment : fragments) {
      fragment.validate();
    }
  }

  public Set<Integer> getWorkers() {
    ImmutableSet.Builder<Integer> workers = ImmutableSet.builder();
    for (final PlanFragmentEncoding fragment : fragments) {
      workers.addAll(fragment.workers);
    }
    return workers.build();
  }
}