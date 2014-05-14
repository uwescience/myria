package edu.washington.escience.myria.parallel;

import java.util.Set;
import java.util.TreeMap;

/**
 * Represents a consistent hash structure.
 */
public final class ConsistentHash {

  /** the lower bound of the circle. */
  private static final int LOWER_BOUND = 0;
  /** the upper bound of the circle. */
  private static final int UPPER_BOUND = Integer.MAX_VALUE;

  /** the consistent hash circle. */
  private static TreeMap<Integer, ConsistentHashInterval> circle;

  /**
   * Prevent instantiating ConsistentHash objects. Constructs the consistent hash structure with the number of workers
   * and replicas.
   * 
   * @param workers the max number of the workers that the system can handle.
   * @param replica the number of replicas per worker.
   */
  private ConsistentHash(final int workers, final int replica) {
    initialize(workers, replica);
  }

  /**
   * Prevent instantiating ConsistentHash objects. Constructs the consistent hash structure with the intervals.
   * 
   * @param intervals the consistent hash intervals.
   */
  private ConsistentHash(final Set<ConsistentHashInterval> intervals) {
    initialize(intervals);
  }

  /**
   * @param intervals the consistent hash intervals.
   */
  private static void initialize(final Set<ConsistentHashInterval> intervals) {
    circle = new TreeMap<Integer, ConsistentHashInterval>();
    for (ConsistentHashInterval interval : intervals) {
      int end = interval.getEnd();
      circle.put(end, interval);
    }
  }

  /**
   * @param workers the max number of the workers that the system can handle.
   * @param replica the number of replicas per worker.
   */
  private static void initialize(final int workers, final int replica) {
    circle = new TreeMap<Integer, ConsistentHashInterval>();
    int totalSize = UPPER_BOUND - LOWER_BOUND;
    int intervalSize = totalSize / (workers * (replica + 1));
    for (int r = 0; r <= replica; r++) {
      for (int n = 0; n < workers; n++) {
        int start = (intervalSize * workers * r) + (n * intervalSize);
        int end = start + intervalSize;
        circle.put(end, new ConsistentHashInterval(start, end, n, r));
      }
    }
  }

  /**
   * Factory method for constructing the consistent hash structure with the workers and replicas.
   * 
   * @param workers the max number of the workers that the system can handle.
   * @param replica the number of replicas per worker.
   * @return the consistent hash object
   */
  public static ConsistentHash of(final int workers, final int replica) {
    return new ConsistentHash(workers, replica);
  }

  /**
   * Factory method for constructing the consistent hash structure with the intervals.
   * 
   * @param intervals the consistent hash intervals.
   * @return the consistent hash object
   */
  public static ConsistentHash of(final Set<ConsistentHashInterval> intervals) {
    return new ConsistentHash(intervals);
  }

  /**
   * @param hashValue the hash value
   * @return The workerId based on the hashValue
   */
  public int getWorkerId(final int hashValue) {
    return getWorkerIdHelper(hashValue);
  }

  /**
   * @param maxWorkers the max number of the workers that the system can handle.
   * @param replicas the number of replicas per worker.
   * @param hashValue the hash value
   * @return The workerId based on the hashValue
   */
  public static int getWorkerId(final int maxWorkers, final int replicas, final int hashValue) {
    initialize(maxWorkers, replicas);
    return getWorkerIdHelper(hashValue);
  }

  /**
   * @param intervals the consistent hash intervals.
   * @param hashValue the hash value
   * @return The workerId based on the hashValue
   */
  public static int getWorkerId(final Set<ConsistentHashInterval> intervals, final int hashValue) {
    initialize(intervals);
    return getWorkerIdHelper(hashValue);
  }

  /**
   * @param hashValue the hash value to hash into the circle
   * @return the worker id that this <code>hashValue</code> belongs to.
   */
  private static int getWorkerIdHelper(final int hashValue) {
    int workerId;
    if (circle.lastKey() < hashValue) {
      workerId = circle.firstEntry().getValue().worker;
    } else {
      workerId = circle.ceilingEntry(hashValue).getValue().worker;
    }
    return workerId;
  }

  /**
   * @param hashValue the hash value
   * @param numCurWorkers the current number of workers in the cluster
   * @return The maxN value based on the hashValue and numCurWorkers
   */
  public int getMaxN(final int hashValue, final int numCurWorkers) {
    return getMaxNHelper(hashValue, numCurWorkers);
  }

  /**
   * @param maxWorkers the max number of the workers that the system can handle.
   * @param replicas the number of replicas per worker.
   * @param hashValue the hash value
   * @param numCurWorkers the current number of workers
   * @return The maxN value based on the hashValue and numCurWorkers
   */
  public static int getMaxN(final int maxWorkers, final int replicas, final int hashValue, final int numCurWorkers) {
    initialize(maxWorkers, replicas);
    return getMaxNHelper(hashValue, numCurWorkers);
  }

  /**
   * @param intervals the consistent hash intervals.
   * @param hashValue the hash value
   * @param numCurWorkers the current number of workers
   * @return The maxN value based on the hashValue and numCurWorkers
   */
  public static int getMaxN(final Set<ConsistentHashInterval> intervals, final int hashValue, final int numCurWorkers) {
    initialize(intervals);
    return getMaxNHelper(hashValue, numCurWorkers);
  }

  /**
   * @param hashValue the hash value to get the maxN.
   * @param numWorkers the number of workers as the base to assign maxN.
   * @return the maxN value of this hashValue
   */
  private static int getMaxNHelper(final int hashValue, final int numWorkers) {
    int maxN = getWorkerIdHelper(hashValue);
    return Math.max(maxN, numWorkers);
  }

  /**
   * A structure to hold a consistent hash interval.
   */
  public static final class ConsistentHashInterval {

    /** the start of the interval. */
    private final int start;
    /** the end of the interval (exclusive of this value). */
    private final int end;
    /** which worker belongs to this interval. */
    private final int worker;
    /** the replica of the <code>worker</code> in which this interval belongs to. */
    private final int replica;

    /**
     * Constructs a consistent hash interval.
     * 
     * @param start the start of the interval.
     * @param end the end of the interval (exclusive).
     * @param worker the workers that this interval belongs to.
     * @param replica the replica of the <code>worker</code> in which this interval belongs to.
     */
    public ConsistentHashInterval(final int start, final int end, final int worker, final int replica) {
      this.start = start;
      this.end = end;
      this.worker = worker;
      this.replica = replica;
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
     * @return the worker
     */
    public int getWorker() {
      return worker;
    }

    /**
     * @return the replica
     */
    public int getReplica() {
      return replica;
    }
  }
}
