package edu.washington.escience.myria.api.encoding;

import java.net.URI;

import edu.washington.escience.myria.util.DateTimeUtils;

/**
 * The encoding for the status of a query.
 */
public class QueryStatusEncoding {
  /** An empty default constructor. */
  public QueryStatusEncoding() {
  }

  /** Constructor. */
  public QueryStatusEncoding(final long queryId) {
    this.queryId = queryId;
  }

  /**
   * Factory for a newly submitted query.
   * 
   * @param rawQuery the raw query submitted to the system.
   * @param logicalRa the logical plan of the query.
   * @param physicalPlan the physical execution plan.
   * @param profilingMode whether the query executed with profiling on.
   * @return a QueryStatusEncoding object containing the submitted query, with the submit time set to
   *         DateTimeUtils.nowInISO8601().
   */
  public static QueryStatusEncoding submitted(final String rawQuery, final String logicalRa, final Object physicalPlan,
      final Boolean profilingMode) {
    QueryStatusEncoding ret = new QueryStatusEncoding();
    ret.rawQuery = rawQuery;
    ret.logicalRa = logicalRa;
    ret.physicalPlan = physicalPlan;
    ret.profilingMode = profilingMode;
    ret.submitTime = DateTimeUtils.nowInISO8601();
    ret.status = Status.ACCEPTED;
    return ret;
  }

  /** The URL of this resource. */
  public URI url;
  /** The ID of this query. */
  public Long queryId;
  /** The raw query submitted to the system. */
  public String rawQuery;
  /** The logical plan. */
  public String logicalRa;
  /** The physical execution plan. */
  public Object physicalPlan;
  /** The submit time of this query. */
  public String submitTime;
  /** The start time of this query. */
  public String startTime;
  /** The finish time of this query. */
  public String finishTime;
  /** The error message of this query. */
  public String message;
  /** The execution time of this query (in nanoseconds). */
  public Long elapsedNanos;
  /** The status of the query. */
  public Status status;
  /** The profilingMode of the query. */
  public Boolean profilingMode;

  /** The current status of the query. */
  public static enum Status {
    ACCEPTED, RUNNING, SUCCESS, KILLED, ERROR, UNKNOWN
  }
}