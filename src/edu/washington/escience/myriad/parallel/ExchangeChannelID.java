package edu.washington.escience.myriad.parallel;

import java.util.Arrays;

/**
 * 
 * An ExchangeChannel represents a partition of a {@link Consumer}/{@link Producer} operator.
 * 
 * It's an input ExchangeChannel if it's a partition of a {@link Consumer}. Otherwise, an output ExchangeChannel.
 * 
 * */
public class ExchangeChannelID implements Comparable<ExchangeChannelID> {

  final int remoteID;
  final long operatorID;
  final String toStringValue;

  public ExchangeChannelID(final long operatorID, final int remoteID) {
    this.remoteID = remoteID;
    this.operatorID = operatorID;
    toStringValue = "opID[" + operatorID + "],rmtID[" + remoteID + "]";
  }

  @Override
  public String toString() {
    return toStringValue;
  }

  @Override
  public int hashCode() {
    return (int) operatorID * 31 + remoteID;
  }

  @Override
  public int compareTo(final ExchangeChannelID o) {
    if (operatorID != o.operatorID) {
      return (int) (operatorID - o.operatorID);
    }
    return remoteID - o.remoteID;
  }

  @Override
  public boolean equals(final Object oo) {
    if (oo == null) {
      return false;
    }
    ExchangeChannelID o = (ExchangeChannelID) oo;
    if (this == o) {
      return true;
    }
    if (operatorID == o.operatorID && remoteID == o.remoteID) {
      return true;
    }
    return false;
  }

  public static void main(String[] args) {
    ExchangeChannelID[] array = new ExchangeChannelID[10];
    array[0] = new ExchangeChannelID(0, 3);
    array[1] = new ExchangeChannelID(1, 1);
    array[2] = new ExchangeChannelID(1, 2);
    array[3] = new ExchangeChannelID(1, 0);
    array[4] = new ExchangeChannelID(0, 4);
    array[5] = new ExchangeChannelID(0, 0);
    array[6] = new ExchangeChannelID(1, 3);
    array[7] = new ExchangeChannelID(0, 2);
    array[8] = new ExchangeChannelID(1, 4);
    array[9] = new ExchangeChannelID(0, 1);
    Arrays.sort(array);
    for (ExchangeChannelID id : array) {
      System.out.println(id);
    }
    System.out.println();
    System.out.println(array[Arrays.binarySearch(array, new ExchangeChannelID(1, 1))]);
  }

}
