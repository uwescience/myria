package edu.washington.escience.myria.parallel;

import javax.annotation.Nullable;

import com.google.common.base.Objects;

import edu.washington.escience.myria.proto.QueryProto;
import edu.washington.escience.myria.util.DateTimeUtils;

/**
 * This data structure records the various statistics of the execution of a query or subquery.
 */
public class ExecutionStatistics {

  /** Constant to use when times have not been set. */
  private static final long MISSING_TIME = -1;

  /**
   * Start timestamp. This timestamp is machine specific.
   */
  private transient volatile long startAtInNano = MISSING_TIME;

  /**
   * End timestamp. This timestamp is machine specific.
   */
  private transient volatile long endAtInNano = MISSING_TIME;

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
  @Nullable
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
    Long elapsed = Objects.firstNonNull(getQueryExecutionElapse(), MISSING_TIME);
    return QueryProto.ExecutionStatistics.newBuilder().setElapse(elapsed).build();
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

  /**
   * Resets the start and end times measured by this object.
   */
  public final void reset() {
    startAtInNano = MISSING_TIME;
    endAtInNano = MISSING_TIME;
    startTime = null;
    endTime = null;
  }
}
