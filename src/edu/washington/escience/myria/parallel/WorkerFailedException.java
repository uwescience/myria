package edu.washington.escience.myria.parallel;

/**
 * The exception for failure of a REEF evaluator/context/task corresponding to a Myria worker.
 * */
public class WorkerFailedException extends Exception {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  /**
   * Default constructor.
   * */
  public WorkerFailedException() {}

  /**
   * @param cause the Throwable causing this Exception.
   * */
  public WorkerFailedException(final Throwable cause) {
    super(cause);
  }
}
