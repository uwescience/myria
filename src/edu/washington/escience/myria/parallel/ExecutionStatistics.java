package edu.washington.escience.myria.parallel;

import javax.annotation.Nullable;

import org.joda.time.DateTime;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.proto.QueryProto;

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

  /** Start time. */
  private transient volatile DateTime startTime;

  /** End time. */
  private transient volatile DateTime endTime;

  /**
   * Set the start time to now.
   */
  public final void markStart() {
    Preconditions.checkArgument(startTime == null, "can't re-mark start");
    startAtInNano = System.nanoTime();
    startTime = DateTime.now();
  }

  /**
   * Set the end time to now.
   */
  public final void markEnd() {
    Preconditions.checkArgument(endTime == null, "can't re-mark end");
    endAtInNano = System.nanoTime();
    endTime = DateTime.now();
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
    Long elapsed = MoreObjects.firstNonNull(getQueryExecutionElapse(), MISSING_TIME);
    return QueryProto.ExecutionStatistics.newBuilder().setElapse(elapsed).build();
  }

  /**
   * @return the start time, in ISO8601 datetime format.
   */
  protected final DateTime getStartTime() {
    return startTime;
  }

  /**
   * @return the end time, in ISO8601 datetime format.
   */
  protected final DateTime getEndTime() {
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
