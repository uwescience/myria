package edu.washington.escience.myriad.parallel.ipc;

import edu.washington.escience.myriad.parallel.Consumer;
import edu.washington.escience.myriad.parallel.Producer;

/**
 * 
 * An {@link StreamIOChannel} represents a partition of a {@link Consumer}/{@link Producer} operator.
 * 
 * It's an input {@link StreamIOChannel} if it's a partition of a {@link Consumer}. Otherwise, an output
 * {@link StreamIOChannel}.
 * 
 * */
public class StreamIOChannelID implements Comparable<StreamIOChannelID> {

  /**
   * remote worker ID.
   * */
  private final int remoteID;

  /**
   * operator id.
   * */
  private final long operatorID;

  /**
   * used in toString.
   * */
  private final String toStringValue;

  /**
   * @param operatorID operator ID.
   * @param remoteID worker ID.
   * */
  public StreamIOChannelID(final long operatorID, final int remoteID) {
    this.remoteID = remoteID;
    this.operatorID = operatorID;
    toStringValue = "opID[" + operatorID + "],rmtID[" + remoteID + "]";
  }

  @Override
  public final String toString() {
    return toStringValue;
  }

  @Override
  public final int hashCode() {
    return (int) operatorID * MAGIC_HASHCODE_BASE + remoteID;
  }

  /**
   * for generating hash code.
   * */
  private static final int MAGIC_HASHCODE_BASE = 31;

  @Override
  public final int compareTo(final StreamIOChannelID o) {
    if (operatorID != o.operatorID) {
      return (int) (operatorID - o.operatorID);
    }
    return remoteID - o.remoteID;
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
    if (operatorID == o.operatorID && remoteID == o.remoteID) {
      return true;
    }
    return false;
  }

}
