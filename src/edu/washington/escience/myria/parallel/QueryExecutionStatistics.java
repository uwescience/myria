package edu.washington.escience.myria.parallel;

import edu.washington.escience.myria.proto.QueryProto.ExecutionStatistics;
import edu.washington.escience.myria.util.DateTimeUtils;

/**
 * This data structure records the various statistics of the execution of a query partition.
 * */
public class QueryExecutionStatistics {

  /**
   * Start timestamp of the query partition. This timestamp is machine specific.
   * */
  private transient volatile long startAtInNano = 0;

  /**
   * End timestamp of the whole query partition. This timestamp is machine specific.
   * */
  private transient volatile long endAtInNano = -1;

  /** Query start time, in ISO8601 datetime format. */
  private transient volatile String startTime;

  /** Query end time, in ISO8601 datetime format. */
  private transient volatile String endTime;

  /**
   * Call at the time the query starts.
   * */
  public final void markQueryStart() {
    startAtInNano = System.nanoTime();
    startTime = DateTimeUtils.nowInISO8601();
  }

  /**
   * Call at the time the query ends.
   * */
  public final void markQueryEnd() {
    endAtInNano = System.nanoTime();
    endTime = DateTimeUtils.nowInISO8601();
  }

  /**
   * @return query execution elapsed in nanoseconds.
   * */
  public final long getQueryExecutionElapse() {
    return endAtInNano - startAtInNano;
  }

  /**
   * @return the protobuf message representation of this class.
   * */
  public final ExecutionStatistics toProtobuf() {
    return ExecutionStatistics.newBuilder().setElapse(getQueryExecutionElapse()).build();
  }

  /**
   * @return the start time of this query, in ISO8601 datetime format.
   */
  protected final String getStartTime() {
    return startTime;
  }

  /**
   * @return the end time of this query, in ISO8601 datetime format.
   */
  protected final String getEndTime() {
    return endTime;
  }
}
