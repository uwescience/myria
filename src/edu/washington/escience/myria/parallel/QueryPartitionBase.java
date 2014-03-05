package edu.washington.escience.myria.parallel;


/**
 * Implemented basic life cycle management of query partitions and other common stuff.
 * */
public abstract class QueryPartitionBase implements QueryPartition {

  /**
   * Just started.
   * */
  public static final int STATE_START = 0x00;
  /**
   * Initialized.
   * */
  public static final int STATE_INITIALIZED = 0x01;
  /**
   * Paused.
   * */
  public static final int STATE_PAUSED = 0x02;
  /**
   * Running.
   * */
  public static final int STATE_RUNNING = 0x03;
  /**
   * Killed.
   * */
  public static final int STATE_KILLED = 0x04;
  /**
   * Completed.
   * */
  public static final int STATE_COMPLETED = 0x05;

  /**
   * Query state.
   * */
  private final int state = STATE_START;

  /**
   * Query state lock.
   * */
  private final Object stateLock = new Object();

  /**
   * @param state the state
   * @return name of the state
   * */
  public static String stateToString(final int state) {
    switch (state) {
      case STATE_START:
        return "START";
      case STATE_INITIALIZED:
        return "INITIALIZED";
      case STATE_RUNNING:
        return "RUNNING";
      case STATE_PAUSED:
        return "PAUSED";
      case STATE_KILLED:
        return "KILLED";
      case STATE_COMPLETED:
        return "COMPLETED";
      default:
        return null;
    }
  }

}
