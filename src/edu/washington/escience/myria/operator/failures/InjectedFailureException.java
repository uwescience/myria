package edu.washington.escience.myria.operator.failures;

import edu.washington.escience.myria.DbException;

/**
 * Exception denoting a injected failure.
 * */
public class InjectedFailureException extends DbException {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  /**
   * @param message the message describing the exception.
   * */
  public InjectedFailureException(final String message) {
    super(message);
  }

  /**
   * @param e the cause of this Exception.
   * */
  public InjectedFailureException(final Throwable e) {
    super(e);
  }
}
