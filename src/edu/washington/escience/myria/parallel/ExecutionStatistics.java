package edu.washington.escience.myria.parallel;

import edu.washington.escience.myria.proto.QueryProto;
import edu.washington.escience.myria.util.DateTimeUtils;

/**
 * This data structure records the various statistics of the execution of a query or subquery.
 */
public class ExecutionStatistics {

  /**
   * Start timestamp. This timestamp is machine specific.
   */
  private transient volatile long startAtInNano = 0;

  /**
   * End timestamp. This timestamp is machine specific.
   */
  private transient volatile long endAtInNano = -1;

  /** Start time, in ISO8601 datetime format. */
  private transient volatile String startTime;

  /** End time, in ISO8601 datetime format. */
  private transient volatile String endTime;

  /**
   * Set the start time to now.
   */
  public final void markStart() {
    startAtInNano = System.nanoTime();
    startTime = DateTimeUtils.nowInISO8601();
  }

  /**
   * Set the end time to now.
   */
  public final void markEnd() {
    endAtInNano = System.nanoTime();
    endTime = DateTimeUtils.nowInISO8601();
  }

  /**
   * @return the elapsed time, in nanoseconds.
   */
  public final Long getQueryExecutionElapse() {
    if (startTime == null) {
      return null;
    }
    if (endTime == null) {
      return System.nanoTime() - startAtInNano;
    }
    return endAtInNano - startAtInNano;
  }

  /**
   * @return the protobuf message representation of this class.
   */
  public final QueryProto.ExecutionStatistics toProtobuf() {
    return QueryProto.ExecutionStatistics.newBuilder().setElapse(getQueryExecutionElapse()).build();
  }

  /**
   * @return the start time, in ISO8601 datetime format.
   */
  protected final String getStartTime() {
    return startTime;
  }

  /**
   * @return the end time, in ISO8601 datetime format.
   */
  protected final String getEndTime() {
    return endTime;
  }
}
