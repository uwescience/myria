package edu.washington.escience.myria.perfenforce;

import org.slf4j.LoggerFactory;

public final class PerfEnforceException extends Exception {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      LoggerFactory.getLogger(PerfEnforceException.class);

  public PerfEnforceException() {
    super();
  }

  public PerfEnforceException(final String message) {
    super(message);
  }

  public PerfEnforceException(final Throwable cause) {
    super(cause);
  }
}
