package edu.washington.escience.myria.parallel;

/**
 * The exception for losing heartbeats from a worker.
 * */
public class LostHeartbeatException extends InterruptedException {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  /**
   * Default constructor.
   * */
  public LostHeartbeatException() {}

  /**
   * @param s the message describing the detail of this Exception.
   * */
  public LostHeartbeatException(final String s) {
    super(s);
  }
}
