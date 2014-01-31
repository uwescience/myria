package edu.washington.escience.myria.scaling;

/**
 * Represents an interval for the Consistent Hashing algorithm.
 * 
 * @author vaspol
 */
public final class ConsistentHashInterval implements Comparable<ConsistentHashInterval> {

  /** The magic number for hash code. */
  private static final int HASHCODE_MAGIC_NUMBER = 243;
  /** The start of the interval. */
  private int start;
  /** The end of the interval. */
  private int end;
  /** The worker id that this interval belongs to. */
  private int workerId;
  /** The partition that this interval is assigned to. */
  private int partition;

  /**
   * Constructs an interval with start and end point.
   * 
   * @param start the start of the interval.
   * @param end the end of the interval.
   * @param workerId the worker id of this interval.
   * @param partition the partition of this interval.
   */
  public ConsistentHashInterval(final int start, final int end, final int workerId, final int partition) {
    this.start = start;
    this.end = end;
    this.workerId = workerId;
    this.partition = partition;
  }

  /**
   * @return the start
   */
  public int getStart() {
    return start;
  }

  /**
   * @return the end
   */
  public int getEnd() {
    return end;
  }

  /**
   * @param start the start to set
   */
  public void setStart(final int start) {
    this.start = start;
  }

  /**
   * @param end the end to set
   */
  public void setEnd(final int end) {
    this.end = end;
  }

  /**
   * @return the workerId
   */
  public int getWorkerId() {
    return workerId;
  }

  /**
   * @return the partition
   */
  public int getPartition() {
    return partition;
  }

  /**
   * @param workerId the workerId to set
   */
  public void setWorkerId(final int workerId) {
    this.workerId = workerId;
  }

  /**
   * @param partition the partition to set
   */
  public void setPartition(final int partition) {
    this.partition = partition;
  }

  /**
   * Implements the compareTo function.
   * 
   * @param o The object the be compared.
   * @return If the start of this object is greater will return a value greater than 0. Otherwise, this method will
   *         return a negative value. Compares the method using the start of the interval first, then the end of the
   *         interval.
   */
  @Override
  public int compareTo(final ConsistentHashInterval o) {
    ConsistentHashInterval interval = o;
    if (start == interval.start) {
      return start - interval.start;
    } else {
      return end - interval.end;
    }
  }

  /**
   * @param o the object to be compared to
   * 
   * @return Whether this object is equals to o
   */
  @Override
  public boolean equals(final Object o) {
    if (o instanceof ConsistentHashInterval) {
      ConsistentHashInterval interval = (ConsistentHashInterval) o;
      return interval.start == start && interval.end == end;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = HASHCODE_MAGIC_NUMBER * hash + start;
    hash = HASHCODE_MAGIC_NUMBER * hash + end;
    return hash;
  }

  /**
   * @return String representation of this object
   */
  @Override
  public String toString() {
    return "start: " + start + ", end: " + end;
  }
}
