package edu.washington.escience.myria.parallel;

import java.io.Serializable;

import edu.washington.escience.myria.proto.ControlProto;

/**
 * A simple wrapper that wraps the socket information of both workers and the server (coordinator).
 */
public final class ResourceStats implements Serializable {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** timestamp of this statistics. */
  public long timestamp;
  /** operator Id. */
  public int opId;
  /** measurement. */
  public String measurement;
  /** value. */
  public long value;
  /** query Id. */
  public long queryId;
  /** subquery Id. */
  public long subqueryId;

  /** empty constructor, needed for jersey to build response. */
  public ResourceStats() {
  }

  /**
   * 
   * @param timestamp timestamp
   * @param opId opId
   * @param measurement measurement
   * @param value value
   * @param queryId queryId
   * @param subqueryId subqueryId
   */
  public ResourceStats(final long timestamp, final int opId, final String measurement, final long value,
      final long queryId, final long subqueryId) {
    this.timestamp = timestamp;
    this.opId = opId;
    this.measurement = measurement;
    this.value = value;
    this.queryId = queryId;
    this.subqueryId = subqueryId;
  }

  @Override
  public String toString() {
    return timestamp + " " + opId + " " + measurement + " " + value + " " + queryId + "." + subqueryId;
  }

  /**
   * @return the protobuf message representation of this class.
   * */
  public ControlProto.ResourceStats toProtobuf() {
    return ControlProto.ResourceStats.newBuilder().setTimestamp(timestamp).setOpId(opId).setMeasurement(measurement)
        .setValue(value).setQueryId(queryId).setSubqueryId(subqueryId).build();
  }

  /**
   * @param stats the resource stats from protobuf.
   * @return a translated ResourceStats object.
   * */
  public static ResourceStats fromProtobuf(final ControlProto.ResourceStats stats) {
    return new ResourceStats(stats.getTimestamp(), stats.getOpId(), stats.getMeasurement(), stats.getValue(), stats
        .getQueryId(), stats.getSubqueryId());
  }
}