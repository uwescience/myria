package edu.washington.escience.myria.parallel.ipc;

import java.io.Serializable;

/**
 *
 * ID of a stream I/O channel.
 *
 * */
public class StreamIOChannelID implements Comparable<StreamIOChannelID>, Serializable {

  /**
   *
   */
  private static final long serialVersionUID = -5362137244233871013L;

  /**
   * remote worker ID.
   * */
  private final int remoteID;

  /**
   * operator id.
   * */
  private final long streamID;

  /**
   * used in toString.
   * */
  private final String toStringValue;

  /**
   * @param streamID stream ID.
   * @param remoteID worker ID.
   * */
  public StreamIOChannelID(final long streamID, final int remoteID) {
    this.remoteID = remoteID;
    this.streamID = streamID;
    toStringValue = "(opID:" + streamID + ",rmtID:" + remoteID + ")";
  }

  @Override
  public final String toString() {
    return toStringValue;
  }

  @Override
  public final int hashCode() {
    return (int) streamID * MAGIC_HASHCODE_BASE + remoteID;
  }

  /**
   * for generating hash code.
   * */
  private static final int MAGIC_HASHCODE_BASE = 31;

  @Override
  public final int compareTo(final StreamIOChannelID o) {
    if (streamID != o.streamID) {
      return (int) (streamID - o.streamID);
    }
    return remoteID - o.remoteID;
  }

  /**
   * @return the remoteID of this channel.
   * */
  public final int getRemoteID() {
    return remoteID;
  }

  /**
   * @return the streamID of this channel.
   * */
  public final long getStreamID() {
    return streamID;
  }

  @Override
  public final boolean equals(final Object oo) {
    if (oo == null) {
      return false;
    }
    StreamIOChannelID o = (StreamIOChannelID) oo;
    if (this == o) {
      return true;
    }
    if (streamID == o.streamID && remoteID == o.remoteID) {
      return true;
    }
    return false;
  }
}
