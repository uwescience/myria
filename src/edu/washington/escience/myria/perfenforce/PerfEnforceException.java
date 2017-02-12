package edu.washington.escience.myria.perfenforce;

import org.slf4j.LoggerFactory;

/**
 * The PerfEnforceException class. Only thrown when running PerfEnforce features.
 */
public final class PerfEnforceException extends Exception {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      LoggerFactory.getLogger(PerfEnforceException.class);

  /**
   * The PerfEnforceException constructor.
   */
  public PerfEnforceException() {
    super();
  }

  /**
   * The PerfEnforceException constructor that takes a message as input.
   * @param message the message describing the error
   */
  public PerfEnforceException(final String message) {
    super(message);
  }
}
