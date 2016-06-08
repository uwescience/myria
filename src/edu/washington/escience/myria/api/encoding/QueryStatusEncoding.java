package edu.washington.escience.myria.api.encoding;

import java.net.URI;
import java.util.List;

import org.joda.time.DateTime;

import edu.washington.escience.myria.MyriaConstants.FTMode;
import edu.washington.escience.myria.MyriaConstants.ProfilingMode;
import edu.washington.escience.myria.api.encoding.plan.SubPlanEncoding;

/**
 * The encoding for the status of a query.
 */
public class QueryStatusEncoding {
  /** An empty default constructor. */
  public QueryStatusEncoding() {}

  /** Constructor. */
  public QueryStatusEncoding(final long queryId) {
    this.queryId = queryId;
  }

  /**
   * Factory for a newly submitted query.
   *
   * @param rawQuery the raw query submitted to the system.
   * @param logicalRa the logical plan of the query.
   * @param plan the physical execution plan.
   * @param profilingMode which profiling mode the query is executed with.
   * @return a QueryStatusEncoding object containing the submitted query, with the submit time set to
   *         DateTimeUtils.nowInISO8601().
   */
  public static QueryStatusEncoding submitted(final QueryEncoding query) {
    QueryStatusEncoding ret = new QueryStatusEncoding();
    ret.rawQuery = query.rawQuery;
    ret.logicalRa = query.logicalRa;
    ret.plan = query.plan;
    ret.profilingMode = query.profilingMode;
    ret.ftMode = query.ftMode;
    ret.submitTime = DateTime.now();
    ret.status = Status.ACCEPTED;
    ret.language = query.language;
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
  public SubPlanEncoding plan;
  /** The submit time of this query. */
  public DateTime submitTime;
  /** The start time of this query. */
  public DateTime startTime;
  /** The finish time of this query. */
  public DateTime finishTime;
  /** The error message of this query. */
  public String message;
  /** The execution time of this query (in nanoseconds). */
  public Long elapsedNanos;
  /** The status of the query. */
  public Status status;
  /** The profilingMode of the query. */
  public List<ProfilingMode> profilingMode;
  /** The ftMode of the query. */
  public FTMode ftMode;
  /** The language of the query. */
  public String language;

  /** The current status of the query. */
  public static enum Status {
    ACCEPTED,
    RUNNING,
    SUCCESS,
    KILLING,
    KILLED,
    ERROR,
    UNKNOWN;

    /**
     * Return {@code true} if a query with the given status is ongoing, i.e., it can be killed or completed.
     *
     * @param s a status variable
     * @return {@code true} if a query with the given status is ongoing.
     */
    public static boolean ongoing(final Status s) {
      return s == ACCEPTED || s == RUNNING;
    }

    /**
     * Return {@code true} if a query with the given status is completely finished and the status will not change.
     *
     * @param s a status variable
     * @return {@code true} if a query with the given status is completely finished and the status will not change.
     */
    public static boolean finished(final Status s) {
      return s == SUCCESS || s == KILLED || s == ERROR || s == UNKNOWN;
    }
  }
}
