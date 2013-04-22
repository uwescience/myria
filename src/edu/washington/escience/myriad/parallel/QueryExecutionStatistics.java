package edu.washington.escience.myriad.parallel;

import edu.washington.escience.myriad.proto.QueryProto.ExecutionStatistics;

/**
 * This datastructure recording the various statistics of the execution of a query partition.
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

  /**
   * Call at the time the query starts.
   * */
  public final void markQueryStart() {
    startAtInNano = System.nanoTime();
  }

  /**
   * Call at the time the query ends.
   * */
  public final void markQueryEnd() {
    endAtInNano = System.nanoTime();
  }

  /**
   * @return query execution elapse in nano seconds.
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

}
